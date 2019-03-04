import asyncio
import traceback
import logging

from quart import Quart, request

from . import ClusterError, ClusterContext, ClusterCtl

logging.basicConfig(level=logging.DEBUG)

CONFIG_DIR = '/home/woky/rchain/20wip/public-testnet/data/config'

app = Quart(__name__)
cluster = ClusterCtl(ClusterContext(CONFIG_DIR, kill_jobs=True))

@app.route('/supervisor/notify/nodes/<node>/<event>', methods=['POST'])
async def handle_node_notify(node, event):

    req = {'node': node, 'action': action, 'args': request.args.to_dict()}
    try:
        await cluster.dispatch(req)
        return '', 202
    except ClusterError as e:
        return str(e), 400
    except Exception as e:
        traceback.print_exc()
        return '', 500
