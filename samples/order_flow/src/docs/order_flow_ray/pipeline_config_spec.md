# Pipeline Configuration Specification

## Overview

The pipeline uses a hierarchical configuration structure with **executable instances** and **step-aware input/output mapping**. Each pipeline step can discover its own input files and knows where to write outputs based on the StorageConfig mapping.

## Key Features

- **Step Independence**: Each step can run independently by discovering its own input files
- **Input/Output Mapping**: StorageConfig automatically maps step instances to their input/output locations
- **Flexible Execution**: Run any subset of steps (normalization only, feature engineering only, etc.)
- **Type Safety**: Uses isinstance() for robust step identification

## Configuration Structure

```
PipelineConfig (top-level)
├── DataConfig (raw data paths, date ranges, exchanges)
├── ProcessingConfig (executable step instances)
│   ├── normalization: Normalizer | None
│   ├── feature_engineering: FeatureEngineer | None
│   ├── model_training: Trainer | None
│   ├── model_inference: Predictor | None
│   └── model_backtest: Backtester | None
├── StorageConfig (step input/output mapping)
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

All pipeline steps must implement a `discover_files()` method for input file discovery:

- **Normalizer**: Transforms raw data to normalized schema
  - Method: `normalize(raw_data: pl.LazyFrame, data_type: str) -> pl.LazyFrame`
  - Method: `discover_files(data_access, input_path: str, file_sort_order: str) -> List[str]`
  
- **FeatureEngineer**: Computes features from normalized data
  - Method: `engineer_features(normalized_data: pl.LazyFrame) -> pl.LazyFrame`
  - Method: `discover_files(data_access, input_path: str, file_sort_order: str) -> List[str]`
  
- **Trainer**: Trains ML models on features
  - Method: `train(features: pl.LazyFrame) -> Model`
  - Method: `discover_files(data_access, input_path: str, file_sort_order: str) -> List[str]`
  
- **Predictor**: Generates predictions using trained models
  - Method: `predict(features: pl.LazyFrame, model: Model) -> pl.LazyFrame`
  - Method: `discover_files(data_access, input_path: str, file_sort_order: str) -> List[str]`
  
- **Backtester**: Evaluates strategy performance
  - Method: `backtest(predictions: pl.LazyFrame) -> BacktestResults`
  - Method: `discover_files(data_access, input_path: str, file_sort_order: str) -> List[str]`

### StorageConfig

Defines S3 locations for each pipeline stage and provides automatic input/output mapping for step instances.

```python
@dataclass(frozen=True)
class StorageConfig:
    raw_data: S3Location
    normalized: S3Location
    features: S3Location
    models: S3Location
    predictions: S3Location
    backtest: S3Location
    
    def get_step_input_output(self, step_instance) -> Tuple[S3Location, S3Location]:
        """Return (input_location, output_location) for a given step instance."""
        from data_preprocessing.data_normalization import BMLLNormalizer
        from feature_engineering.order_flow import FeatureEngineering
        from model_training.base import ModelTraining
        from model_inference.base import ModelInference
        from model_backtest.base import ModelBacktest
        
        if isinstance(step_instance, BMLLNormalizer):
            return (self.raw_data, self.normalized)
        elif isinstance(step_instance, FeatureEngineering):
            return (self.normalized, self.features)
        elif isinstance(step_instance, ModelTraining):
            return (self.features, self.models)
        elif isinstance(step_instance, ModelInference):
            return (self.features, self.predictions)
        elif isinstance(step_instance, ModelBacktest):
            return (self.predictions, self.backtest)
        else:
            raise ValueError(f"Unknown step type: {type(step_instance)}")
    
    def get_step_input(self, step_instance) -> S3Location:
        """Return input location for a given step instance."""
        return self.get_step_input_output(step_instance)[0]
    
    def get_step_output(self, step_instance) -> S3Location:
        """Return output location for a given step instance."""
        return self.get_step_input_output(step_instance)[1]
```

**Step Flow Mapping:**
- `BMLLNormalizer`: raw_data → normalized
- `FeatureEngineering`: normalized → features  
- `ModelTraining`: features → models
- `ModelInference`: features → predictions
- `ModelBacktest`: predictions → backtest

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
        raw_data=S3Location(path="s3://bucket/raw/"),
        normalized=S3Location(path="s3://bucket/intermediate/normalized"),
        features=S3Location(path="s3://bucket/intermediate/features"),
        models=S3Location(path="s3://bucket/output/models"),
        predictions=S3Location(path="s3://bucket/output/predictions"),
        backtest=S3Location(path="s3://bucket/output/backtest")
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
        normalization=None,  # Skip - use pre-normalized data
        feature_engineering=OrderFlowFeatureEngineer(lookback=10),
        model_training=None,
        model_inference=None,
        model_backtest=None
    ),
    storage=StorageConfig(
        raw_data=S3Location(path="s3://bucket/raw/"),
        normalized=S3Location(path="s3://bucket/intermediate/normalized"),
        features=S3Location(path="s3://bucket/intermediate/features"),
        models=S3Location(path="s3://bucket/output/models"),
        predictions=S3Location(path="s3://bucket/output/predictions"),
        backtest=S3Location(path="s3://bucket/output/backtest")
    ),
    ray=RayConfig(...)
)
```

**Pipeline will:**
1. Discover normalized files from `s3://bucket/intermediate/normalized`
2. Apply feature engineering and write to `s3://bucket/intermediate/features`
3. Skip all other steps

## Step-Aware Pipeline Execution

```python
def _discover_files(self):
    # Get first active step
    active_steps = self._get_active_steps()
    if not active_steps:
        raise ValueError("No processing steps configured")
    
    first_step_name, first_step_instance = active_steps[0]
    
    # Get input path using isinstance mapping
    input_location = self.config.storage.get_step_input(first_step_instance)
    
    # Let the step discover its own input files
    return first_step_instance.discover_files(
        self.data_access, 
        input_location.path, 
        self.config.ray.file_sort_order
    )

def _get_active_steps(self) -> List[Tuple[str, Any]]:
    """Return list of (step_name, step_instance) for configured steps."""
    steps = []
    for step_name in ['normalization', 'feature_engineering', 'model_training', 'model_inference', 'model_backtest']:
        step = getattr(self.config.processing, step_name, None)
        if step is not None:
            steps.append((step_name, step))
    return steps
```

## File Discovery Examples

**Normalization Step:**
```python
# BMLLNormalizer.discover_files() scans raw_data path
# Looks for: s3://bucket/raw/2024/01/01/XNAS/trades/*.parquet
files = normalizer.discover_files(data_access, "s3://bucket/raw/", "desc")
```

**Feature Engineering Step:**
```python
# FeatureEngineering.discover_files() scans normalized path  
# Looks for: s3://bucket/intermediate/normalized/**/*.parquet
files = feature_eng.discover_files(data_access, "s3://bucket/intermediate/normalized", "desc")
```

## Benefits

1. **Step Independence**: Each step can run independently by discovering its own input files
2. **Flexible Execution**: Run any subset of steps without dependencies
3. **Type Safety**: isinstance() provides robust step identification
4. **Automatic I/O Mapping**: StorageConfig handles input/output routing
5. **Testability**: Easy to test individual steps in isolation
6. **Composability**: Mix and match steps for different pipeline runs

## Architecture Improvements

**Before:**
- Pipeline hardcoded to use normalization for file discovery
- Steps couldn't run independently
- Required all previous steps to be configured

**After:**
- Each step discovers its own input files
- StorageConfig provides automatic input/output mapping
- Can run any subset of steps (normalization only, feature engineering only, etc.)
- Type-safe step identification with isinstance()

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
