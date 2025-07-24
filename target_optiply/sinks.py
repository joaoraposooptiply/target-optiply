"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import json
import logging
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase

from target_optiply.auth import OptiplyAuthenticator
from target_optiply.client import OptiplySink

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder for datetime objects."""

    def default(self, obj):
        """Encode datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class BaseOptiplySink(OptiplySink):
    """Base sink for Optiply streams."""

    endpoint = None
    field_mappings = {}
    _processed_records = []

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = self.stream_name.lower() if not self.endpoint else self.endpoint
        self._processed_records = []

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess the record before sending to API."""
        # Build attributes using field mappings
        attributes = self.build_attributes(record, self.get_field_mappings())

        # Add any additional attributes from the record
        self._add_additional_attributes(record, attributes)

        payload = {
            "data": {
                "type": self.endpoint,
                "attributes": attributes
            }
        }
        
        # Add ID for PATCH requests
        if "id" in record:
            payload["data"]["id"] = record["id"]
            
        return payload

    def upsert_record(self, record: dict, context: dict) -> tuple:
        """Process the record and return (id, success, state_updates)."""
        state_updates = {}
        
        try:
            # Set http_method based on presence of id field
            http_method = "PATCH" if "id" in record else "POST"
            
            # Log record processing
            self.logger.info(f"Processing record for {self.stream_name} (ID: {record.get('id')})")

            # For POST requests, check mandatory fields
            if http_method == "POST":
                mandatory_fields = self.get_mandatory_fields()
                missing_fields = []
                for field in mandatory_fields:
                    if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                        missing_fields.append(field)
                if missing_fields:
                    error_msg = f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}"
                    self.logger.error(error_msg)
                    return None, False, state_updates

            # Get the URL for the request
            if record.get('id'):
                endpoint = f"{self.endpoint}/{record.get('id')}"
            else:
                endpoint = self.endpoint
                
            self.logger.info(f"Making {http_method} request to: {endpoint}")

            # Make the request
            response = self.request_api(
                http_method=http_method,
                endpoint=endpoint,
                request_data=record
            )
            
            self.logger.info(f"Response status: {response.status_code}")
            self.logger.info(f"Response body: {response.text}")

            # Handle response
            if response.status_code == 404:
                error_msg = f"Record not found (404): {record.get('id')}"
                self.logger.warning(error_msg)
                return None, False, state_updates
            elif response.status_code >= 400:
                error_msg = f"Request failed with status {response.status_code}: {response.text}"
                self.logger.error(error_msg)
                return None, False, state_updates

            # Parse response to get ID
            response_data = response.json()
            if "data" in response_data and "id" in response_data["data"]:
                record_id = response_data["data"]["id"]
            else:
                record_id = record.get("id", "unknown")

            self.logger.info(f"{self.stream_name} processed with id: {record_id}")
            return record_id, True, state_updates

        except Exception as e:
            error_msg = f"Error processing record: {str(e)}"
            self.logger.error(error_msg)
            return None, False, state_updates

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return self.field_mappings

    def build_attributes(self, record: Dict, field_mappings: Dict[str, str]) -> Dict:
        """Build attributes dictionary from record using field mappings.

        Args:
            record: The record to transform
            field_mappings: Dictionary mapping record fields to API fields

        Returns:
            Dictionary of attributes for the API request
        """
        attributes = {}
        from datetime import datetime
        from decimal import Decimal
        for record_field, api_field in field_mappings.items():
            if record_field in record and record[record_field] is not None:
                value = record[record_field]
                # Handle datetime objects
                if isinstance(value, datetime):
                    value = value.isoformat()
                elif isinstance(value, Decimal):
                    value = float(value)
                attributes[api_field] = value
        return attributes

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        # Handle emails field - convert from JSON string to array if present
        if "emails" in record and record["emails"] is not None:
            try:
                attributes["emails"] = json.loads(record["emails"])
            except json.JSONDecodeError:
                self.logger.warning(f"Could not parse emails JSON string: {record['emails']}")
                attributes["emails"] = []

        # Fields that should be integers
        integer_fields = [
            "deliveryTime",
            "userReplenishmentPeriod",
            "lostSalesReaction",
            "lostSalesMovReaction",
            "backorderThreshold",
            "backordersReaction",
            "maxLoadCapacity",
            "containerVolume"
        ]
        for field in integer_fields:
            if field in record and record[field] is not None:
                try:
                    # First convert to float to handle decimal strings, then to int
                    attributes[field] = int(float(record[field]))
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {record[field]}")
                    attributes.pop(field, None)

        # Fields that should be floats
        float_fields = [
            "minimumOrderValue",
            "fixedCosts"
        ]
        for field in float_fields:
            if field in record and record[field] is not None:
                try:
                    attributes[field] = float(record[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to float: {record[field]}")
                    attributes.pop(field, None)

        # Validate type field
        if "type" in record and record["type"] not in ["vendor", "producer"]:
            self.logger.warning(f"Invalid type value: {record['type']}. Must be 'vendor' or 'producer'")
            attributes["type"] = "vendor"  # Default to vendor if invalid

        # Validate globalLocationNumber length
        if "globalLocationNumber" in record and len(record["globalLocationNumber"]) != 13:
            self.logger.warning(f"Invalid globalLocationNumber length: {len(record['globalLocationNumber'])}. Must be 13 characters")
            attributes.pop("globalLocationNumber", None)  # Remove if invalid

        # Remove remoteDataSyncedToDate as it's not accepted by the API
        attributes.pop("remoteDataSyncedToDate", None)

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink."""
        return []


class ProductsSink(BaseOptiplySink):
    """Products sink class."""

    endpoint = "products"
    
    @property
    def name(self) -> str:
        return "Products"
    field_mappings = {
        "name": "name",
        "skuCode": "skuCode",
        "eanCode": "eanCode",
        "articleCode": "articleCode",
        "price": "price",
        "unlimitedStock": "unlimitedStock",
        "stockLevel": "stockLevel",
        "notBeingBought": "notBeingBought",
        "resumingPurchase": "resumingPurchase",
        "status": "status",
        "assembled": "assembled",
        "minimumStock": "minimumStock",
        "maximumStock": "maximumStock",
        "ignored": "ignored",
        "manualServiceLevel": "manualServiceLevel",
        "createdAtRemote": "createdAtRemote",
        "stockMeasurementUnit": "stockMeasurementUnit"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "stockLevel", "unlimitedStock"]


class SupplierSink(BaseOptiplySink):
    """Optiply target sink class for suppliers."""

    endpoint = "suppliers"
    
    @property
    def name(self) -> str:
        return "Suppliers"
    field_mappings = {
        "name": "name",
        "emails": "emails",
        "minimumOrderValue": "minimumOrderValue",
        "fixedCosts": "fixedCosts",
        "deliveryTime": "deliveryTime",
        "userReplenishmentPeriod": "userReplenishmentPeriod",
        "reactingToLostSales": "reactingToLostSales",
        "lostSalesReaction": "lostSalesReaction",
        "lostSalesMovReaction": "lostSalesMovReaction",
        "backorders": "backorders",
        "backorderThreshold": "backorderThreshold",
        "backordersReaction": "backordersReaction",
        "maxLoadCapacity": "maxLoadCapacity",
        "containerVolume": "containerVolume",
        "ignored": "ignored",
        "globalLocationNumber": "globalLocationNumber",
        "type": "type"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name"]


class SupplierProductSink(BaseOptiplySink):
    """Optiply target sink class for supplier products."""

    endpoint = "supplierProducts"
    
    @property
    def name(self) -> str:
        return "SupplierProducts"
    field_mappings = {
        "name": "name",
        "skuCode": "skuCode",
        "eanCode": "eanCode",
        "articleCode": "articleCode",
        "price": "price",
        "minimumPurchaseQuantity": "minimumPurchaseQuantity",
        "lotSize": "lotSize",
        "availability": "availability",
        "availabilityDate": "availabilityDate",
        "preferred": "preferred",
        "productId": "productId",
        "supplierId": "supplierId",
        "deliveryTime": "deliveryTime",
        "status": "status",
        "freeStock": "freeStock",
        "weight": "weight",
        "volume": "volume"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "productId", "supplierId"]


class BuyOrderSink(BaseOptiplySink):
    """Optiply target sink class for buy orders."""

    endpoint = "buyOrders"
    
    @property
    def name(self) -> str:
        return "BuyOrders"
    field_mappings = {
        "placed": "placed",
        "completed": "completed",
        "expectedDeliveryDate": "expectedDeliveryDate",
        "totalValue": "totalValue",
        "supplierId": "supplierId",
        "accountId": "accountId",
        "assembly": "assembly"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["placed", "totalValue", "supplierId", "accountId"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        if "line_items" in record:
            line_items = json.loads(record["line_items"])
            buy_order_lines = []
            total_value = 0
            for item in line_items:
                subtotal_value = float(item["subtotalValue"])
                total_value += subtotal_value
                buy_order_lines.append({
                    "type": "buyOrderLines",
                    "attributes": {
                        "quantity": item["quantity"],
                        "subtotalValue": str(subtotal_value),
                        "productId": item["productId"],
                        "expectedDeliveryDate": item.get("expectedDeliveryDate")
                    }
                })
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = buy_order_lines


class BuyOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for buy order lines."""

    endpoint = "buyOrderLines"
    
    @property
    def name(self) -> str:
        return "BuyOrderLines"
    field_mappings = {
        "quantity": "quantity",
        "subtotalValue": "subtotalValue",
        "productId": "productId",
        "buyOrderId": "buyOrderId",
        "expectedDeliveryDate": "expectedDeliveryDate"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["subtotalValue", "productId", "quantity", "buyOrderId"]


class SellOrderSink(BaseOptiplySink):
    """Optiply target sink class for sell orders."""

    endpoint = "sellOrders"
    
    @property
    def name(self) -> str:
        return "SellOrders"
    field_mappings = {
        "placed": "placed",
        "totalValue": "totalValue"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["totalValue", "placed"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        if "line_items" in record:
            line_items = json.loads(record["line_items"])
            sell_order_lines = []
            total_value = 0
            for item in line_items:
                subtotal_value = float(item["subtotalValue"])
                total_value += subtotal_value
                sell_order_lines.append({
                    "type": "sellOrderLines",
                    "attributes": {
                        "quantity": item["quantity"],
                        "subtotalValue": str(subtotal_value),
                        "productId": item["productId"]
                    }
                })
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = sell_order_lines


class SellOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for sell order lines."""

    endpoint = "sellOrderLines"
    
    @property
    def name(self) -> str:
        return "SellOrderLines"
    field_mappings = {
        "quantity": "quantity",
        "subtotalValue": "subtotalValue",
        "productId": "productId",
        "sellOrderId": "sellOrderId"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["subtotalValue", "sellOrderId", "productId", "quantity"]