"""Factory for creating feature engineer instances."""


class FeatureEngineerFactory:
    """Factory for creating feature engineer instances."""
    
    @staticmethod
    def create(engineer_type: str, **kwargs):
        """Create feature engineer instance.
        
        Args:
            engineer_type: Type of feature engineer
            **kwargs: Additional arguments for the engineer
            
        Returns:
            FeatureEngineer instance
        """
        if engineer_type == 'order_flow':
            from .order_flow import OrderFlowFeatureEngineer
            return OrderFlowFeatureEngineer(**kwargs)
        else:
            raise ValueError(f"Unknown engineer type: {engineer_type}")
