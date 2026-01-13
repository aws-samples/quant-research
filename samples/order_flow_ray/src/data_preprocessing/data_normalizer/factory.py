from .base import DataNormalizer
from .bmll import BMLLNormalizer

class NormalizerFactory:
    """Factory for creating data normalizers."""
    
    _normalizers = {
        "bmll": BMLLNormalizer
    }
    
    @classmethod
    def create(cls, source: str) -> DataNormalizer:
        """Create normalizer for data source."""
        if source not in cls._normalizers:
            raise ValueError(f"Unknown data source: {source}")
        return cls._normalizers[source]()
    
    @classmethod
    def register(cls, source: str, normalizer_class: type):
        """Register new normalizer."""
        cls._normalizers[source] = normalizer_class