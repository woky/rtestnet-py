import sys
import asyncio
import traceback

from dataclasses import dataclass
from typing import Dict
from asyncio import Task, create_task, CancelledError
from asyncio.subprocess import Process, create_subprocess_exec

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
        try:
            _REQUEST_SCHEMA.validate(req)
        except SchemaError as e:
            raise ClusterCtlError('Invalid requst: ' + str(e)) from e

        key = self._req_key(req)
        job = self._jobs.pop(key, _Job(key, req))
        if job.task:
            job.task.cancel()
            if job.process and self.ctx.kill_jobs:
                job.process.terminate()
        job.task = create_task(self._run_job(job))
        self._jobs[key] = job

    async def _run_job(self, job: _Job):
        try:
            if job.process:
                await job.process.wait()
                job.process = None

            # XXX make sure the process is killed if cancel happens here
            job.process = await create_subprocess_exec(
                *self._get_node_ctl_cmd(job.req),
                stdout=sys.stdout,
                stderr=sys.stderr
            )
            await job.process.wait()
            del self._jobs[job.key]
        except CancelledError as e:
            raise
        except:
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
