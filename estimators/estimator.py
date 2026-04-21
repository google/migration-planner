from typing import Any, Dict, List
import math

class Estimator:
    def __init__(self):
        pass

    def calculate_resource_count(self, data: Dict[str, Any], failures: List[Dict[str, str]]) -> Dict[str, int]:
        raise NotImplementedError("Subclasses must implement the estimate method")
    
    def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
        """Calculates duration in HOURS based on batching throughput constraints."""
        item_counts = data.get("item_counts", [])
        batch_size = data.get("batch_size", 100)
        global_limit = data.get("global_limit", 100)
        batch_time = data.get("batch_time", 1)
        user_limit = data.get("user_limit", 1)
        
        active_counts = [c for c in item_counts if c > 0]
        if not active_counts:
            return 0.0

        batch_counts = [math.ceil(c / batch_size) for c in active_counts]
        batch_counts.sort()

        total_seconds = 0.0
        previous_level = 0
        n = len(batch_counts)

        for i, current_level in enumerate(batch_counts):
            delta = current_level - previous_level
            if delta > 0:
                active_users = n - i
                max_user_capacity = active_users * user_limit
                effective_concurrency = min(global_limit, max_user_capacity)
                current_throughput = effective_concurrency / batch_time
                total_layer_batches = delta * active_users
                seconds_for_layer = total_layer_batches / current_throughput
                total_seconds += seconds_for_layer
            previous_level = current_level

        return total_seconds / 3600.0
    
    def get_resource_type(self) -> str:
        raise NotImplementedError("Subclasses must implement the get_resource_type method")
    
    def get_migration_type(self) -> str:
        raise NotImplementedError("Subclasses must implement the get_migration_type method")
    
    def shutdown(self):
        if self.archive_executor:
            self.archive_executor.shutdown(wait=False)
