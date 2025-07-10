"""Optiply target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from typing import Dict, Any
import json
import os

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


class TargetOptiply(Target):
    """Target for Optiply API."""

    name = "target-optiply"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            description="Optiply API username",
            required=True,
        ),
        th.Property(
            "client_id",
            th.StringType,
            description="Optiply API client ID",
            required=True,
        ),
        th.Property(
            "client_secret",
            th.StringType,
            description="Optiply API client secret",
            required=True,
        ),
        th.Property(
            "password",
            th.StringType,
            description="Optiply API password",
            required=True,
        ),
        th.Property(
            "account_id",
            th.IntegerType,
            description="Optiply account ID",
        ),
        th.Property(
            "coupling_id",
            th.IntegerType,
            description="Optiply coupling ID",
        ),
        th.Property(
            "start_date",
            th.StringType,
            description="Start date for data sync",
        ),
        th.Property(
            "hotglue_metadata",
            th.ObjectType(
                th.Property(
                    "metadata",
                    th.ObjectType(
                        th.Property(
                            "webshop_handle",
                            th.StringType,
                            description="Webshop handle",
                        ),
                    ),
                ),
            ),
            description="Hotglue metadata",
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str):
        """Get sink class for the given stream name."""
        if stream_name == "BuyOrders":
            return BuyOrderSink
        elif stream_name == "Products":
            return ProductsSink
        elif stream_name == "Suppliers":
            return SupplierSink
        elif stream_name == "SupplierProducts":
            return SupplierProductSink
        elif stream_name == "BuyOrderLines":
            return BuyOrderLineSink
        elif stream_name == "SellOrders":
            return SellOrderSink
        elif stream_name == "SellOrderLines":
            return SellOrderLineSink
        else:
            # Return base sink for unknown streams
            return BaseOptiplySink

    def process_batch(self, context: Dict) -> None:
        """Process a batch of records."""
        for stream_name, stream in self.streams.items():
            if stream_name in self.sinks:
                sink = self.sinks[stream_name]
                for record in stream.records:
                    sink.process_record(record, context)

        # Generate detailed summary
        self.logger.info("\n=== Processing Summary ===")
        total_success = 0
        total_failure = 0
        export_summary = {}
        export_details = {}
        
        for stream_name, sink in self.sinks.items():
            stats = sink.get_stats()
            self.logger.info(f"\n{stream_name}:")
            self.logger.info(f"  Successfully processed: {stats['success']}")
            self.logger.info(f"  Failed records: {stats['failure']}")
            self.logger.info(f"  Total records: {stats['total']}")
            
            # Add to totals
            total_success += stats['success']
            total_failure += stats['failure']
            
            # Add to export summary
            export_summary[stream_name] = {
                "success": stats['success'],
                "fail": stats['failure'],
                "existing": 0,  # Not tracked in current implementation
                "updated": 0    # Not tracked in current implementation
            }
            
            # Add to export details
            export_details[stream_name] = stats['processed_records']
        
        self.logger.info("\nOverall Summary:")
        self.logger.info(f"  Total successfully processed: {total_success}")
        self.logger.info(f"  Total failed records: {total_failure}")
        self.logger.info(f"  Total records processed: {total_success + total_failure}")
        self.logger.info("======================\n")

        # Create state with job results
        state = {
            "bookmarks": export_details,
            "summary": export_summary
        }
        
        # Save state to target_state.json (Hotglue-style)
        state_file = os.path.join(os.getcwd(), "target-state.json")
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        # Emit state message to stdout (Singer protocol)
        self.state = state
        self._write_state_message()
        
        # Update job-details.json if it exists
        job_details_file = os.path.join(os.getcwd(), "job-details.json")
        if os.path.exists(job_details_file):
            with open(job_details_file, 'r') as f:
                job_details = json.load(f)
            
            # Update metrics
            if isinstance(job_details, list) and len(job_details) > 0:
                job_details[0]["metrics"] = {
                    "recordCount": {},
                    "exportSummary": export_summary,
                    "exportDetails": export_details
                }
                
                with open(job_details_file, 'w') as f:
                    json.dump(job_details, f, indent=2)


if __name__ == "__main__":
    TargetOptiply.cli()
