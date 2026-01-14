# Component Structure and Naming Conventions

## Overview

All pipeline components follow a consistent structure and naming convention to maintain code organization and predictability.

## Naming Convention

### Package Names

Use **noun form** (not verb form) for package names:

✅ **Correct:**
- `data_normalization` (not `data_normalizer`)
- `feature_engineering` (not `feature_engineer`)
- `model_training` (not `model_trainer`)
- `model_inference` (not `model_predictor`)
- `backtesting` (not `backtester`)

❌ **Incorrect:**
- `data_normalizer`
- `feature_engineer`
- `model_trainer`

### Class Names

Use **agent/actor form** for class names:

✅ **Correct:**
- `Normalizer` (in `data_normalization` package)
- `FeatureEngineer` (in `feature_engineering` package)
- `Trainer` (in `model_training` package)
- `Predictor` (in `model_inference` package)
- `Backtester` (in `backtesting` package)

## Standard Component Structure

Every component package follows this structure:

```
component_name/
├── __init__.py          # Exports public API
├── base.py              # Abstract base class defining interface
├── factory.py           # Factory for creating instances
└── <implementation>.py  # Specific implementations
```

### Example: data_normalization

```
data_normalization/
├── __init__.py
├── base.py              # DataNormalizer (abstract base)
├── factory.py           # NormalizerFactory
├── bmll.py              # BMLLNormalizer
├── polygon.py           # PolygonNormalizer
└── normalized_schema.py # Shared schema definitions
```

### Example: feature_engineering

```
feature_engineering/
├── __init__.py
├── base.py              # FeatureEngineer (abstract base)
├── factory.py           # FeatureEngineerFactory
├── order_flow.py        # OrderFlowFeatureEngineer
└── microstructure.py    # MicrostructureFeatureEngineer
```

### Example: model_training

```
model_training/
├── __init__.py
├── base.py              # Trainer (abstract base)
├── factory.py           # TrainerFactory
├── xgboost.py           # XGBoostTrainer
└── lightgbm.py          # LightGBMTrainer
```

## File Responsibilities

### base.py

Defines the abstract interface that all implementations must follow.

```python
from abc import ABC, abstractmethod

class Normalizer(ABC):
    """Abstract base class for data normalizers."""
    
    @abstractmethod
    def normalize(self, raw_data, data_type):
        """Normalize raw data to standard schema."""
        pass
```

### factory.py

Provides factory method for creating instances by name.

```python
class NormalizerFactory:
    """Factory for creating normalizer instances."""
    
    @staticmethod
    def create(source_type: str):
        if source_type == 'bmll':
            from .bmll import BMLLNormalizer
            return BMLLNormalizer()
        elif source_type == 'polygon':
            from .polygon import PolygonNormalizer
            return PolygonNormalizer()
        else:
            raise ValueError(f"Unknown source type: {source_type}")
```

### <implementation>.py

Concrete implementation of the interface.

```python
from .base import Normalizer

class BMLLNormalizer(Normalizer):
    """BMLL-specific normalizer implementation."""
    
    def normalize(self, raw_data, data_type):
        # Implementation
        pass
```

### __init__.py

Exports public API for the package.

```python
from .base import Normalizer
from .factory import NormalizerFactory
from .bmll import BMLLNormalizer
from .polygon import PolygonNormalizer

__all__ = [
    'Normalizer',
    'NormalizerFactory',
    'BMLLNormalizer',
    'PolygonNormalizer',
]
```

## Component Mapping

| Package Name | Base Class | Factory | Example Implementation |
|--------------|------------|---------|------------------------|
| `data_normalization` | `Normalizer` | `NormalizerFactory` | `BMLLNormalizer` |
| `feature_engineering` | `FeatureEngineer` | `FeatureEngineerFactory` | `OrderFlowFeatureEngineer` |
| `model_training` | `Trainer` | `TrainerFactory` | `XGBoostTrainer` |
| `model_inference` | `Predictor` | `PredictorFactory` | `ModelPredictor` |
| `backtesting` | `Backtester` | `BacktesterFactory` | `VectorizedBacktester` |

## Usage Examples

### Using Factory

```python
from data_normalization import NormalizerFactory

normalizer = NormalizerFactory.create('bmll')
normalized_data = normalizer.normalize(raw_data, 'trades')
```

### Direct Instantiation

```python
from data_normalization import BMLLNormalizer

normalizer = BMLLNormalizer()
normalized_data = normalizer.normalize(raw_data, 'trades')
```

### In Pipeline Config

```python
from data_normalization import BMLLNormalizer
from feature_engineering import OrderFlowFeatureEngineer

config = PipelineConfig(
    processing=ProcessingConfig(
        normalization=BMLLNormalizer(),
        feature_engineering=OrderFlowFeatureEngineer(lookback=10)
    )
)
```

## Benefits

1. **Consistency**: All components follow same structure
2. **Predictability**: Know where to find base class, factory, implementations
3. **Discoverability**: Easy to find and understand components
4. **Maintainability**: Clear separation of concerns
5. **Extensibility**: Easy to add new implementations

## Migration Notes

Existing components need to be renamed:

- `data_normalizer/` → `data_normalization/`
- `data_access/` → Keep as-is (utility, not a pipeline step)
- Future components follow this convention from the start
