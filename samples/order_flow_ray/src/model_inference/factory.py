# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Factory for creating predictor instances."""


class PredictorFactory:
    """Factory for creating predictor instances."""
    
    @staticmethod
    def create(predictor_type: str, **kwargs):
        """Create predictor instance.
        
        Args:
            predictor_type: Type of predictor
            **kwargs: Additional arguments for the predictor
            
        Returns:
            Predictor instance
        """
        if predictor_type == 'model':
            from .model_predictor import ModelPredictor
            return ModelPredictor(**kwargs)
        else:
            raise ValueError(f"Unknown predictor type: {predictor_type}")
