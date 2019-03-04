import asyncio
import traceback

from quart import Quart, request

#import rtestnet.common as common
from . import ClusterError, ClusterContext, ClusterCtl

CONFIG_DIR = '/home/woky/rchain/20wip/public-testnet/data/config'

app = Quart(__name__)
cluster = ClusterCtl(ClusterContext(CONFIG_DIR))

#async def post_message(msg):
#    redis = await common.create_redis_from_env()
#    await redis.publish_json(common.CONTROL_CHANNEL, msg)
#    redis.close()
#    await redis.wait_closed()


@app.route('/nodes/<node>/<action>', methods=['POST'])
async def handle_node_ctl(*, node, action):
    req = {'node': node, 'action': action, 'args': request.args.to_dict()}
    try:
        await cluster.dispatch(req)
        return '', 202
    except ClusterError as e:
        return str(e), 400
    except Exception as e:
        traceback.print_exc()
        return '', 500
