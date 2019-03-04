from .. import ClusterContext
from . import NodeError, NodeContext, NodeConfig, NodeFiles
from .ops import NodeOpsGCE


class NodeCtlError(NodeError):
    pass


class NodeCtl:
    def __init__(self, ctx: NodeContext):
        self.ctx = ctx

    def _load_config(self):
        config = NodeConfig.load(
            self.ctx.cluster.config_defaults_file,
            self.ctx.config_override_file, self.ctx.config_state_file
        )
        if 'gce_name' not in config:
            prefix = config.get('gce_name_prefix', '')
            config['gce_name'] = prefix + self.ctx.name
        return config

    def _get_node_ops(self):
        return NodeOpsGCE(self._load_config())

    def stop(self, clean=False, mrproper=False):
        self._get_node_ops().stop(clean=clean, mrproper=mrproper)
        if self.ctx.config_state_file.exists():
            self.ctx.config_state_file.unlink()

    def start(self):
        ops = self._get_node_ops()
        NodeFiles(self.ctx, ops.config).update()
        ops.start()
        ops.config.save(self.ctx.config_state_file)

    def restart(self, clean=False, mrproper=False):
        self.stop(clean=clean, mrproper=mrproper)
        self.start()

    def make_leader(self):
        self._get_node_ops().add_dns_rec('boot', 0)
