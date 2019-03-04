import os.path
import json

from dataclasses import dataclass
from pathlib import Path

from pyhocon import ConfigFactory
from deepmerge import always_merger as merger

from . import NodeContext, NodeConfig

def _mtime(path: Path):
    if path.exists():
        return os.path.getmtime(path)
    return -1


@dataclass
class NodeFiles:

    ctx: NodeContext
    config: NodeConfig

    def update(self):
        self._update_rnode_conf()
        #self._update_tls_key()
        #self._update_tls_cert()
        #self._update_rdoctor_ini()

    @property
    def _node_dir(self):
        return

    def _update_rnode_conf(self):
        defaults_file = self.ctx.cluster.conf_dir / 'rnode.conf'
        override_file = self.ctx.conf_dir / 'rnode.override.conf'
        output_file = self.ctx.conf_dir / 'rnode.conf'

        output_file_t = _mtime(override_file)
        if (
            self.config.mtime > output_file_t and (
                'rnode_config' in self.config or
                'rnode_config_override' in self.config
            ) or
            max(_mtime(defaults_file), _mtime(override_file)) > output_file_t
        ):
            output = {}
            if 'rnode_config' in self.config:
                output = self.config['rnode_config']
            else:
                defaults = {}
                if defaults_file.exists():
                    defaults = ConfigFactory.parse_file(defaults_file)
                override = {}
                if 'rnode_config_override' in self.config:
                    override = self.config['rnode_config_override']
                elif override_file.exists():
                    override = ConfigFactory.parse_file(override_file)
                output = merger.merge(defaults, override)
            output_file.write_text(json.dumps(outout))
