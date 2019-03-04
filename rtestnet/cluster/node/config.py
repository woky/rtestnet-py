import json
import os, os.path

from pathlib import Path

from pyhocon import ConfigFactory
from schema import Schema, SchemaError, And, Use, Optional as Opt
from deepmerge import always_merger as merger

_CONFIG_SCHEMA = Schema(
    {
        'gce_zone': And(str, len),
        'gce_machine_type': And(str, len),
        'gce_boot_image': And(str, len),
        'gce_vpc_net': And(str, len),
        'gce_vpc_subnet': And(str, len),
        'gce_tags': [And(str, len)],
        'gdns_zone': And(str, len),
        'gdns_domain': And(str, len),
        'data_disk_size': And(Use(int), lambda n: n > 0),
        Opt('data_disk_type_ssd', default=False): bool,
        Opt('gce_name'): And(str, len),
        Opt('gce_name_prefix', default=''): str,
        Opt('rnode_tls_key'): And(str, len),
        Opt('rdoctor_key'): And(str, len),
        Opt('rnode_config'): Use(ConfigFactory.parse_string),
        Opt('rnode_config_override'): Use(ConfigFactory.parse_string),
    }
)


class NodeConfig(dict):
    def __init__(self, mtime, kvs):
        self.mtime = mtime
        self.update(kvs)

    @staticmethod
    def try_load_file(path: Path, parse=json.loads, default={}):
        kvs, mtime = default, -1
        if path.exists():
            kvs = parse(path.read_text())
            mtime = os.path.getmtime(path)
        return kvs, mtime

    @classmethod
    def load(cls, *paths: Path):
        mtime, kvs = -1, {}
        for p in paths:
            _kvs, _mtime = cls.try_load_file(p)
            if _mtime > mtime:
                mtime = _mtime
            kvs = merger.merge(kvs, _kvs)
        if mtime < 0:
            raise NodeCtlError(
                'No configuration file found. Tried:\n' +
                '\n'.join(['  {}'.format(p) for p in paths])
            )
        try:
            _CONFIG_SCHEMA.validate(kvs)
        except SchemaError as e:
            raise NodeCtlError('Invalid configuration') from e
        return cls(mtime, kvs)

    def save(self, path: Path):
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(self))
        os.utime(path, (self.mtime, ) * 2)
