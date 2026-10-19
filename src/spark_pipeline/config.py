"""Configuration loading for the pipeline."""

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class PipelineConfig:
    app_name: str
    source_path: str
    lake_root: str
    shuffle_partitions: int
    min_order_amount: float

    @property
    def bronze_path(self) -> str:
        return str(Path(self.lake_root) / "bronze" / "orders")

    @property
    def silver_path(self) -> str:
        return str(Path(self.lake_root) / "silver" / "orders")

    @property
    def quarantine_path(self) -> str:
        return str(Path(self.lake_root) / "quarantine" / "orders")

    @property
    def gold_path(self) -> str:
        return str(Path(self.lake_root) / "gold" / "daily_sales")

    @property
    def audit_path(self) -> str:
        return str(Path(self.lake_root) / "audit" / "pipeline_runs")


def load_config(path: str) -> PipelineConfig:
    """Load and validate pipeline settings from a YAML file."""
    with Path(path).open(encoding="utf-8") as config_file:
        values = yaml.safe_load(config_file)
    return PipelineConfig(**values)
