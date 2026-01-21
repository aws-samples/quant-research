"""Factory for feature engineering creation."""


class FeatureEngineeringFactory:
    """Factory for feature engineering creation."""
    
    @staticmethod
    def creation(engineering_type: str, **kwargs):
        """Feature engineering creation.
        
        Args:
            engineering_type: Type of feature engineering
            **kwargs: Additional arguments for the engineering
            
        Returns:
            FeatureEngineering instance
        """
        if engineering_type == 'order_flow':
            from .order_flow import L2QFeatureEngineering, TradeFeatureEngineering
            return L2QFeatureEngineering(**kwargs)
        else:
            raise ValueError(f"Unknown engineering type: {engineering_type}")
