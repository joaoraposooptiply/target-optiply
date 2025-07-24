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
        super().__init__(config, parse_env_config, validate_config)

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


if __name__ == "__main__":
    TargetOptiply.cli()
