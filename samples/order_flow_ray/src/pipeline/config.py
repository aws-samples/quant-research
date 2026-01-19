"""Pipeline configuration classes."""
from dataclasses import dataclass
from typing import Any
from abc import ABC, abstractmethod


class StorageLocation(ABC):
    """Base class for storage location configuration."""
    
    @abstractmethod
    def get_access_type(self) -> str:
        """Return access type ('s3' or 's3tables')."""
        pass
    
    @abstractmethod
    def get_path(self) -> str:
        """Return storage path."""
        pass


@dataclass(frozen=True)
class S3Location(StorageLocation):
    """S3 storage location.
    
    Args:
        path: S3 path (e.g., 's3://bucket/path')
    """
    path: str
    
    def get_access_type(self) -> str:
        return 's3'
    
    def get_path(self) -> str:
        return self.path


@dataclass(frozen=True)
class S3TablesLocation(StorageLocation):
    """S3 Tables storage location.
    
    Args:
        table_name: Table name
        table_bucket_arn: S3 Tables bucket ARN
        namespace: Table namespace
    """
    table_name: str
    table_bucket_arn: str
    namespace: str
    
    def get_access_type(self) -> str:
        return 's3tables'
    
    def get_path(self) -> str:
        return self.table_name


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
    """Storage configuration with granular access control.
    
    Args:
        raw_data: Input data location
        normalized: Normalized data location
        features: Feature data location
        models: Model storage location
        predictions: Predictions storage location
        backtest: Backtest results location
    """
    raw_data: StorageLocation
    normalized: StorageLocation
    features: StorageLocation
    models: StorageLocation
    predictions: StorageLocation
    backtest: StorageLocation


@dataclass(frozen=True)
class RayConfig:
    """Ray runtime configuration.
    
    Args:
        runtime_env: Ray runtime environment dict
        resources: Optional resource requirements per task
        memory_multiplier: Multiplier for file size to estimate memory (default 3.0)
        memory_per_core_gb: Memory per core in GB (default 2.0)
        cpu_buffer: Additional CPUs to add to calculated requirement (default 1)
    """
    runtime_env: dict[str, Any]
    resources: dict[str, Any] | None = None
    memory_multiplier: float = 2.0
    memory_per_core_gb: float = 4.0
    cpu_buffer: int = 1


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
        profile_name: AWS profile name (optional)
    """
    region: str
    data: DataConfig
    processing: ProcessingConfig
    storage: StorageConfig
    ray: RayConfig
    profile_name: str | None = None
