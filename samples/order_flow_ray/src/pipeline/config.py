"""Pipeline configuration classes."""
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DataConfig:
    """Data source configuration.
    
    Args:
        raw_data_path: S3 path to raw BMLL data
        start_date: ISO format date string (YYYY-MM-DD)
        end_date: ISO format date string (YYYY-MM-DD)
        exchanges: List of exchange codes (e.g., ["XNAS", "XNYS"])
        data_types: List of data types (e.g., ["trades", "level2q"])
    """
    raw_data_path: str
    start_date: str
    end_date: str
    exchanges: list[str]
    data_types: list[str]


@dataclass(frozen=True)
class StorageConfig:
    """Storage paths configuration.
    
    Args:
        intermediate_path: S3 path for intermediate results
        output_path: S3 path for final outputs
    """
    intermediate_path: str
    output_path: str
    
    @property
    def normalized_path(self) -> str:
        return f"{self.intermediate_path}/normalized"
    
    @property
    def features_path(self) -> str:
        return f"{self.intermediate_path}/features"
    
    @property
    def models_path(self) -> str:
        return f"{self.output_path}/models"
    
    @property
    def predictions_path(self) -> str:
        return f"{self.output_path}/predictions"
    
    @property
    def backtest_path(self) -> str:
        return f"{self.output_path}/backtest"


@dataclass(frozen=True)
class RayConfig:
    """Ray runtime configuration.
    
    Args:
        runtime_env: Ray runtime environment dict
        resources: Optional resource requirements per task
    """
    runtime_env: dict[str, Any]
    resources: dict[str, Any] | None = None


@dataclass
class ProcessingConfig:
    """Processing steps configuration.
    
    Each step is an executable instance or None to skip.
    
    Args:
        normalization: Normalizer instance or None
        feature_engineering: FeatureEngineer instance or None
        training: Trainer instance or None
        inference: Predictor instance or None
        backtest: Backtester instance or None
    """
    normalization: Any | None = None
    feature_engineering: Any | None = None
    training: Any | None = None
    inference: Any | None = None
    backtest: Any | None = None


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration.
    
    Args:
        region: AWS region
        data: Data source configuration
        processing: Processing steps configuration
        storage: Storage paths configuration
        ray: Ray runtime configuration
    """
    region: str
    data: DataConfig
    processing: ProcessingConfig
    storage: StorageConfig
    ray: RayConfig
