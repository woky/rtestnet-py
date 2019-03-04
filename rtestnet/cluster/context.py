from pathlib import Path
from dataclasses import dataclass


@dataclass
class ClusterContext:
    conf_dir: Path
    private_dir: Path = None
    kill_jobs = False

    def __post_init__(self):
        if not self.private_dir:
            self.private_dir = self.conf_dir

    @property
    def config_defaults_file(self):
        return self.conf_dir / 'config.json'
