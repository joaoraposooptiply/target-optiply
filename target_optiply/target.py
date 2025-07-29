"""Optiply target class."""
from target_optiply.sinks import (
    BaseOptiplySink,
    ProductsSink,
    SupplierSink,
    SupplierProductSink,
    BuyOrderSink,
    BuyOrderLineSink,
    SellOrderSink,
    SellOrderLineSink,
)

from target_hotglue.target import TargetHotglue
from typing import Callable, Dict, List, Optional, Tuple, Type, Union
from pathlib import Path, PurePath


class TargetOptiply(TargetHotglue):
    """Target for Optiply."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        self.logger.info(f"Target config file: {self.config_file}")
        super().__init__(config, parse_env_config, validate_config)
        
        # Log the config structure after initialization
        self.logger.info(f"Target config keys: {list(self._config.keys())}")
        if "importCredentials" in self._config:
            self.logger.info("✅ Target config has importCredentials")
        if "apiCredentials" in self._config:
            self.logger.info("✅ Target config has apiCredentials")

    SINK_TYPES = [
        BaseOptiplySink,
        ProductsSink,
        SupplierSink,
        SupplierProductSink,
        BuyOrderSink,
        BuyOrderLineSink,
        SellOrderSink,
        SellOrderLineSink,
    ]
    MAX_PARALLELISM = 10
    name = "target-optiply"

    def get_sink_class(self, stream_name: str):
        """Get sink class for the given stream name."""
        # Map stream names to sink classes
        sink_map = {
            "BuyOrders": BuyOrderSink,
            "Products": ProductsSink,
            "Suppliers": SupplierSink,
            "SupplierProducts": SupplierProductSink,
            "BuyOrderLines": BuyOrderLineSink,
            "SellOrders": SellOrderSink,
            "SellOrderLines": SellOrderLineSink,
        }
        
        return sink_map.get(stream_name, BaseOptiplySink)

    def get_state(self) -> dict:
        """Override to provide simplified state with only counts."""
        # Get the original state from parent
        original_state = super().get_state()
        
        # Simplify the bookmarks to only show counts
        if "bookmarks" in original_state:
            simplified_bookmarks = {}
            for stream_name, records in original_state["bookmarks"].items():
                if isinstance(records, list):
                    # Count successes and failures
                    success_count = sum(1 for record in records if record.get("success", False))
                    fail_count = len(records) - success_count
                    
                    simplified_bookmarks[stream_name] = {
                        "total": len(records),
                        "success": success_count,
                        "failed": fail_count
                    }
                else:
                    # Keep as is if not a list
                    simplified_bookmarks[stream_name] = records
            
            original_state["bookmarks"] = simplified_bookmarks
        
        return original_state

    def _get_export_summary(self) -> dict:
        """Override to provide simplified export summary."""
        # Get the original export summary from parent
        original_summary = super()._get_export_summary()
        
        # Simplify the export details to only show counts
        if "exportDetails" in original_summary:
            simplified_details = {}
            for stream_name, records in original_summary["exportDetails"].items():
                if isinstance(records, list):
                    # Count successes and failures
                    success_count = sum(1 for record in records if record.get("success", False))
                    fail_count = len(records) - success_count
                    
                    simplified_details[stream_name] = {
                        "total": len(records),
                        "success": success_count,
                        "failed": fail_count
                    }
                else:
                    # Keep as is if not a list
                    simplified_details[stream_name] = records
            
            original_summary["exportDetails"] = simplified_details
        
        return original_summary

    def _get_export_details(self) -> dict:
        """Override to provide simplified export details."""
        # Get the original export details from parent
        original_details = super()._get_export_details()
        
        # Simplify the export details to only show counts
        simplified_details = {}
        for stream_name, records in original_details.items():
            if isinstance(records, list):
                # Count successes and failures
                success_count = sum(1 for record in records if record.get("success", False))
                fail_count = len(records) - success_count
                
                simplified_details[stream_name] = {
                    "total": len(records),
                    "success": success_count,
                    "failed": fail_count
                }
            else:
                # Keep as is if not a list
                simplified_details[stream_name] = records
        
        return simplified_details

    def _get_metrics(self) -> dict:
        """Override to provide simplified metrics."""
        # Get the original metrics from parent
        original_metrics = super()._get_metrics()
        
        # Simplify the export details to only show counts
        if "exportDetails" in original_metrics:
            simplified_details = {}
            for stream_name, records in original_metrics["exportDetails"].items():
                if isinstance(records, list):
                    # Count successes and failures
                    success_count = sum(1 for record in records if record.get("success", False))
                    fail_count = len(records) - success_count
                    
                    simplified_details[stream_name] = {
                        "total": len(records),
                        "success": success_count,
                        "failed": fail_count
                    }
                else:
                    # Keep as is if not a list
                    simplified_details[stream_name] = records
            
            original_metrics["exportDetails"] = simplified_details
        
        return original_metrics


if __name__ == "__main__":
    TargetOptiply.cli()
