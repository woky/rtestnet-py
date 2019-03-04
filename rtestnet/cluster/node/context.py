from pathlib import Path

from .. import ClusterContext

class NodeContext:
    def __init__(self, cluster_ctx: ClusterContext, name: str):
        self.cluster = cluster_ctx
        self.name = name

    @property
    def conf_dir(self) -> Path:
        return self.cluster.conf_dir / self.name

    @property
    def private_dir(self) -> Path:
        return self.cluster.private_dir / self.name

    @property
    def config_override_file(self) -> Path:
        return self.conf_dir / 'config.json'

    @property
    def config_state_file(self) -> Path:
        return self.private_dir / '_cached.config.json'
