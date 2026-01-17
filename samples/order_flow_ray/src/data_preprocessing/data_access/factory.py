from .base import DataAccess
from .s3 import S3DataAccess
from .s3tables import S3TablesDataAccess

class DataAccessFactory:
    """Factory for creating data access implementations."""
    
    _implementations = {
        "s3": S3DataAccess,
        "s3tables": S3TablesDataAccess
    }
    
    @classmethod
    def create(cls, access_type: str, **kwargs) -> DataAccess:
        """Create data access implementation."""
        if access_type not in cls._implementations:
            raise ValueError(f"Unknown access type: {access_type}")
        return cls._implementations[access_type](**kwargs)
    
    @classmethod
    def register(cls, access_type: str, implementation_class: type):
        """Register new data access implementation."""
        cls._implementations[access_type] = implementation_class