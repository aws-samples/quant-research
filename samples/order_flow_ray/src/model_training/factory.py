"""Factory for creating trainer instances."""


class TrainerFactory:
    """Factory for creating trainer instances."""
    
    @staticmethod
    def create(trainer_type: str, **kwargs):
        """Create trainer instance.
        
        Args:
            trainer_type: Type of trainer
            **kwargs: Additional arguments for the trainer
            
        Returns:
            Trainer instance
        """
        if trainer_type == 'xgboost':
            from .xgboost_trainer import XGBoostTrainer
            return XGBoostTrainer(**kwargs)
        else:
            raise ValueError(f"Unknown trainer type: {trainer_type}")
