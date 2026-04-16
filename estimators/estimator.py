from typing import Any, Dict

class Estimator:
    def __init__(self):
        pass

    def calculate_resource_count(self, data: Dict[str, Any]) -> Dict[str, int]:
        raise NotImplementedError("Subclasses must implement the estimate method")
    
    def calculate_migration_eta(self, data: Dict[str, Any]) -> Dict[str, int]:
        raise NotImplementedError("Subclasses must implement the calculate_migration_eta method")
    
    def get_resource_type(self) -> str:
        raise NotImplementedError("Subclasses must implement the get_resource_type method")
    
    def get_migration_type(self) -> str:
        raise NotImplementedError("Subclasses must implement the get_migration_type method")
    
