import sys
import asyncio
import traceback
import logging, reprlib

from dataclasses import dataclass
from typing import Dict
from asyncio import Task, create_task, CancelledError
from asyncio.subprocess import Process, create_subprocess_exec
from pprint import pprint

from schema import Schema, SchemaError, And, Or, Use, Optional as Opt

from . import ClusterError, ClusterContext

_REQUEST_SCHEMA = Schema(
    {
        'node': And(str, len),
        'action': Or('start', 'stop', 'restart', 'lead'),
        'args': {
            Opt('clean'): Or('data', 'all')
        }
    }
)

logger = logging.getLogger(__name__)


class ClusterCtlError(ClusterError):
    pass


@dataclass
class _Job:
    key: str
    req: Dict
    task: Task = None
    process: Process = None


class ClusterCtl:
    def __init__(self, ctx: ClusterContext):
        self.ctx = ctx
        self._jobs = {}

    def _req_key(self, req: Dict) -> str:
        if req['action'] == 'lead':
            return 'lead'
        return req['node']

    async def dispatch(self, req: Dict):
        if logger.isEnabledFor(logging.INFO):
            logger.info('Received request: %s', reprlib.repr(req))
        try:
            _REQUEST_SCHEMA.validate(req)
        except SchemaError as e:
            logger.info('Rejected invalid request')
            raise ClusterCtlError('Invalid requst: ' + str(e)) from e

        key = self._req_key(req)
        new_job = _Job(key, req)
        logger.debug('Job slot: %s', key)

        if key in self._jobs:
            old_job = self._jobs[key]
            logger.debug('Active job exists in the slot')
            if req == old_job.req and not self.ctx.kill_jobs:
                logger.debug('Ignoring duplicate request')
                return

            logger.debug('Cancelling active job')
            old_job.task.cancel()
            if old_job.process:
                if self.ctx.kill_jobs:
                    logger.debug(
                        'Terminating active job\'s worker process PID=%d',
                        old_job.process.pid
                    )
                    old_job.process.terminate()
                new_job.process = old_job.process
                logger.debug(
                    'Inherited active job\'s worker process PID=%d',
                    new_job.process.pid
                )
            del self._jobs[key]
            logger.debug('Active job removed')

        new_job.task = create_task(self._run_job(new_job))
        self._jobs[key] = new_job
        logger.debug('New job scheduled for the request')

    async def _run_job(self, job: _Job):
        logger.debug('Running job in slot %s', job.key)
        try:
            if job.process:
                logger.debug(
                    'Waiting for inherited worker process PID=%d to finish',
                    job.process.pid
                )
                await job.process.wait()
                job.process = None
                logger.debug('Inherited worker process finished')

            cmd = self._get_node_ctl_cmd(job.req)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Creating worker process: %s', ' '.join(cmd))

            # XXX make sure the process is killed if cancel happens here
            job.process = await create_subprocess_exec(
                *cmd, stdout=sys.stdout, stderr=sys.stderr
            )
            logger.debug('Created worker process PID=%d', job.process.pid)

            await job.process.wait()
            logger.debug(
                'Worker process PID=%d finished with status=%d',
                job.process.pid, job.process.returncode
            )

            del self._jobs[job.key]
        except CancelledError as e:
            logger.debug('Current job was cancelled')
            raise
        except:
            logger.debug(
                'Current job ended with unhandled exception', exc_info=True
            )
            traceback.print_exc()
            if job.key in self._jobs:
                del self._jobs[job.key]
            raise

    def _get_node_ctl_cmd(self, req: Dict):
        CLI_MODULE = 'rtestnet.cluster.node.cli'
        cmd = [
            sys.executable, '-m', CLI_MODULE,
            '-d', str(self.ctx.conf_dir),
            '-p', str(self.ctx.private_dir or self.ctx.conf_dir),
            req['node'], req['action']
        ] # yapf: disable

        if req['action'] == 'stop' or req['action'] == 'restart':
            clean = req['args'].get('clean', None)
            if clean == 'data':
                cmd.append('-c')
            elif clean == 'all':
                cmd.append('-C')

        return cmd
