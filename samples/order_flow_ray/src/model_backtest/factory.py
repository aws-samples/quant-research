"""Factory for creating backtester instances."""


class BacktesterFactory:
    """Factory for creating backtester instances."""
    
    @staticmethod
    def create(backtester_type: str, **kwargs):
        """Create backtester instance.
        
        Args:
            backtester_type: Type of backtester
            **kwargs: Additional arguments for the backtester
            
        Returns:
            Backtester instance
        """
        if backtester_type == 'vectorized':
            from .vectorized import VectorizedBacktester
            return VectorizedBacktester(**kwargs)
        else:
            raise ValueError(f"Unknown backtester type: {backtester_type}")
