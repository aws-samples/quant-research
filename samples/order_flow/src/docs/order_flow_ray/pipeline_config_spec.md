# Pipeline Configuration Specification

## Overview

The pipeline uses a hierarchical configuration structure with **executable instances** rather than configuration objects. This allows direct execution without factory patterns while maintaining flexibility and testability.

## Configuration Structure

```
PipelineConfig (top-level)
├── DataConfig (raw data paths, date ranges, exchanges)
├── ProcessingConfig (executable step instances)
│   ├── normalization: Normalizer | None
│   ├── feature_engineering: FeatureEngineer | None
│   ├── training: Trainer | None
│   ├── inference: Predictor | None
│   └── backtest: Backtester | None
├── StorageConfig (intermediate/output paths)
└── RayConfig (runtime_env, resources)
```

## Design Principles

1. **Executable Instances**: ProcessingConfig contains actual instances ready to execute, not configuration specs
2. **Optional Steps**: Set step to `None` to skip execution
3. **Type Safety**: Use dataclasses with type hints for validation
4. **Immutability**: Config objects should be immutable where possible
5. **Composability**: Mix and match steps for different pipeline runs

## Configuration Classes

### PipelineConfig

Top-level configuration orchestrating all pipeline components.

```python
@dataclass
class PipelineConfig:
    data: DataConfig
    processing: ProcessingConfig
    storage: StorageConfig
    ray: RayConfig
```

### DataConfig

Defines data sources, date ranges, and filtering criteria.

```python
@dataclass(frozen=True)
class DataConfig:
    raw_data_path: str          # S3 path to raw BMLL data
    start_date: str             # ISO format: "2024-01-01"
    end_date: str               # ISO format: "2024-12-31"
    exchanges: list[str]        # e.g., ["XNAS", "XNYS"]
    data_types: list[str]       # e.g., ["trades", "level2q"]
```

### ProcessingConfig

Contains executable instances for each pipeline step. Set to `None` to skip.

```python
@dataclass
class ProcessingConfig:
    normalization: Normalizer | None = None
    feature_engineering: FeatureEngineer | None = None
    training: Trainer | None = None
    inference: Predictor | None = None
    backtest: Backtester | None = None
```

**Step Interfaces:**

- **Normalizer**: Transforms raw data to normalized schema
  - Method: `normalize(raw_data: pl.LazyFrame, data_type: str) -> pl.LazyFrame`
  
- **FeatureEngineer**: Computes features from normalized data
  - Method: `engineer_features(normalized_data: pl.LazyFrame) -> pl.LazyFrame`
  
- **Trainer**: Trains ML models on features
  - Method: `train(features: pl.LazyFrame) -> Model`
  
- **Predictor**: Generates predictions using trained models
  - Method: `predict(features: pl.LazyFrame, model: Model) -> pl.LazyFrame`
  
- **Backtester**: Evaluates strategy performance
  - Method: `backtest(predictions: pl.LazyFrame) -> BacktestResults`

### StorageConfig

Defines paths for intermediate and output data.

```python
@dataclass(frozen=True)
class StorageConfig:
    intermediate_path: str      # S3 path for intermediate results
    output_path: str            # S3 path for final outputs
    
    # Derived paths
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
```

### RayConfig

Ray cluster and runtime configuration.

```python
@dataclass(frozen=True)
class RayConfig:
    runtime_env: dict           # Ray runtime environment (working_dir, etc.)
    resources: dict | None = None  # Resource requirements per task
```

## Usage Examples

### Full Pipeline

```python
config = PipelineConfig(
    data=DataConfig(
        raw_data_path="s3://bucket/raw/",
        start_date="2024-01-01",
        end_date="2024-01-31",
        exchanges=["XNAS"],
        data_types=["trades", "level2q"]
    ),
    processing=ProcessingConfig(
        normalization=BMLLNormalizer(),
        feature_engineering=OrderFlowFeatureEngineer(lookback=10),
        training=XGBoostTrainer(params={"max_depth": 5}),
        inference=ModelPredictor(),
        backtest=VectorizedBacktester()
    ),
    storage=StorageConfig(
        intermediate_path="s3://bucket/intermediate",
        output_path="s3://bucket/output"
    ),
    ray=RayConfig(
        runtime_env={"working_dir": "/path/to/src"}
    )
)

pipeline.run(config)
```

### Inference Only

```python
config = PipelineConfig(
    data=DataConfig(...),
    processing=ProcessingConfig(
        normalization=None,  # Skip - use pre-normalized data
        feature_engineering=OrderFlowFeatureEngineer(lookback=10),
        training=None,  # Skip training
        inference=ModelPredictor(),
        backtest=None  # Skip backtest
    ),
    storage=StorageConfig(...),
    ray=RayConfig(...)
)
```

### Feature Engineering Only

```python
config = PipelineConfig(
    data=DataConfig(...),
    processing=ProcessingConfig(
        normalization=BMLLNormalizer(),
        feature_engineering=OrderFlowFeatureEngineer(lookback=10),
        training=None,
        inference=None,
        backtest=None
    ),
    storage=StorageConfig(...),
    ray=RayConfig(...)
)
```

## Pipeline Execution Flow

```python
def run(self, config: PipelineConfig):
    # Initialize Ray
    ray.init(runtime_env=config.ray.runtime_env)
    
    # Discover files
    files = self.discover_files(config.data.raw_data_path)
    
    # Execute enabled steps
    if config.processing.normalization:
        normalized = self.normalize_step(files, config)
    
    if config.processing.feature_engineering:
        features = self.feature_engineering_step(normalized, config)
    
    if config.processing.training:
        model = self.training_step(features, config)
    
    if config.processing.inference:
        predictions = self.inference_step(features, model, config)
    
    if config.processing.backtest:
        results = self.backtest_step(predictions, config)
    
    ray.shutdown()
```

## Benefits

1. **Direct Execution**: No factory pattern needed - just call methods on instances
2. **Flexibility**: Pass any object implementing the interface (duck typing)
3. **Testability**: Easy to inject mocks for testing
4. **Simplicity**: No config-to-instance translation layer
5. **Type Safety**: Type hints catch errors at development time
6. **Reusability**: Create different configs for different runs

## Trade-offs

**Pros:**
- Simple and Pythonic
- Maximum flexibility
- Easy to test and mock
- No boilerplate factory code

**Cons:**
- Not JSON-serializable (configs must be built in Python)
- Can't easily save/load full pipeline configs
- Requires all dependencies at config creation time

**Note**: If JSON serialization is needed later, can adopt Nautilus ImportableConfig pattern with module paths and parameters.
