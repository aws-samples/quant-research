"""Pipeline orchestrator for managing sharding and parallel execution."""
import ray
from typing import Any
from dataclasses import dataclass
from .config import PipelineConfig
from .pipeline import Pipeline


@dataclass
class ShardConfig:
    """Configuration for a single shard.
    
    Args:
        shard_id: Unique identifier for this shard
        date: Date to process (YYYY-MM-DD)
        exchange: Exchange code
        data_types: List of data types to process
    """
    shard_id: str
    date: str
    exchange: str
    data_types: list[str]


class PipelineOrchestrator:
    """Orchestrates parallel pipeline execution across shards."""
    
    def __init__(self, base_config: PipelineConfig):
        """Initialize orchestrator.
        
        Args:
            base_config: Base pipeline configuration
        """
        self.base_config = base_config
    
    def run(self) -> list[Any]:
        """Execute pipelines in parallel across all shards.
        
        Returns:
            List of results from all shards
        """
        # Initialize Ray
        if not ray.is_initialized():
            ray.init(runtime_env=self.base_config.ray.runtime_env)
        
        try:
            # Generate shards based on parallelization strategy
            shards = self._generate_shards()
            print(f"Generated {len(shards)} shards")
            
            # Create Ray remote function for pipeline execution
            @ray.remote
            def run_pipeline_shard(shard: ShardConfig, base_config: PipelineConfig) -> Any:
                """Run pipeline for a single shard."""
                from .pipeline import Pipeline
                from .config import PipelineConfig, DataConfig
                
                # Create shard-specific config
                shard_config = PipelineConfig(
                    region=base_config.region,
                    data=DataConfig(
                        raw_data_path=f"{base_config.data.raw_data_path}/{shard.date.replace('-', '/')}/",
                        start_date=shard.date,
                        end_date=shard.date,
                        exchanges=[shard.exchange],
                        data_types=shard.data_types
                    ),
                    processing=base_config.processing,
                    storage=base_config.storage,
                    ray=base_config.ray
                )
                
                # Run pipeline for this shard
                pipeline = Pipeline(shard_config)
                result = pipeline.run()
                
                return {
                    'shard_id': shard.shard_id,
                    'date': shard.date,
                    'exchange': shard.exchange,
                    'result': result
                }
            
            # Submit all shard tasks
            futures = [run_pipeline_shard.remote(shard, self.base_config) for shard in shards]
            
            # Get results
            results = ray.get(futures)
            
            print(f"Completed {len(results)} shards")
            return results
            
        finally:
            if ray.is_initialized():
                ray.shutdown()
    
    def _generate_shards(self) -> list[ShardConfig]:
        """Generate shards based on date and exchange parallelization.
        
        Returns:
            List of shard configurations
        """
        shards = []
        
        # Parse date range
        dates = self._generate_date_range(
            self.base_config.data.start_date,
            self.base_config.data.end_date
        )
        
        # Create shard for each date-exchange combination
        for date in dates:
            for exchange in self.base_config.data.exchanges:
                shard_id = f"{date}_{exchange}"
                shards.append(ShardConfig(
                    shard_id=shard_id,
                    date=date,
                    exchange=exchange,
                    data_types=self.base_config.data.data_types
                ))
        
        return shards
    
    def _generate_date_range(self, start_date: str, end_date: str) -> list[str]:
        """Generate list of dates between start and end.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of date strings
        """
        from datetime import datetime, timedelta
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates
