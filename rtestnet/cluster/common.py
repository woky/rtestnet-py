import os
import pathlib

import aioredis
import schema

######################################################################

CONTROL_CHANNEL = 'control'
#WORKER_CHANNEL  = 'worker'
WORKER_CHANNEL  = CONTROL_CHANNEL

def get_config_dir():
    return pathlib.Path(os.environ['CONFIG_STORAGE'])

async def create_redis_from_env():
    redis_url = os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')
    return await aioredis.create_redis(redis_url)
