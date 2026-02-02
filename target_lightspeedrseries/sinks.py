"""LightspeedRSeries target sink class, which handles writing streams."""

from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any, Optional
from target_lightspeedrseries.client import LightspeedRSeriesSink
import singer

LOGGER = singer.get_logger()

class BuyOrders(LightspeedRSeriesSink):
    """Order sink for LightspeedRSeries."""
    
    name = "BuyOrders"
    endpoint = "/Order.json"
    
    def _format_date(self, date_value: Any) -> Optional[str]:
        """Convert date value to ISO8601 string format with timezone.
        
        Formats dates as ISO8601 with timezone (e.g., "2025-12-05T08:55:00+00:00").
        
        Args:
            date_value: Can be a datetime object, ISO string, or None
            
        Returns:
            ISO8601 formatted string with timezone or None
        """
        if date_value is None:
            return None
        
        try:
            if isinstance(date_value, str):
                # If it's already a string, try to parse and normalize it
                # Handle ISO format with timezone
                if "Z" in date_value:
                    # Replace Z with +00:00
                    return date_value.replace("Z", "+00:00")
                elif "+" in date_value or date_value.count("-") > 2:
                    # It's an ISO string with timezone, return as is
                    return date_value
                else:
                    # No timezone, assume UTC and add +00:00
                    # Try to parse and add timezone
                    try:
                        dt = datetime.fromisoformat(date_value)
                        if dt.tzinfo is None:
                            # Add UTC timezone
                            dt = dt.replace(tzinfo=timezone.utc)
                        return dt.isoformat()
                    except:
                        # If parsing fails, return as is
                        return date_value
            elif isinstance(date_value, datetime):
                # If it's a datetime object, convert to ISO format with timezone
                dt = date_value
                if dt.tzinfo is None:
                    # Add UTC timezone if not present
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            else:
                # For any other type, convert to string
                return str(date_value)
        except Exception as e:
            LOGGER.warning(f"Failed to format date '{date_value}': {e}, using string conversion")
            return str(date_value)
    
    def _parse_line_items(self, line_items_field: Any) -> List[Dict[str, Any]]:
        """Parse line_items from JSON string or return as list."""
        if not line_items_field:
            return []
        if isinstance(line_items_field, str):
            try:
                return json.loads(line_items_field)
            except json.JSONDecodeError:
                LOGGER.warning(f"Failed to parse line_items as JSON: {line_items_field[:100] if len(str(line_items_field)) > 100 else line_items_field}")
                return []
        if isinstance(line_items_field, list):
            return line_items_field
        return []
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Prepare the Order payload and store line items for later processing."""
        line_items = self._parse_line_items(record.get("line_items"))
        payload = {}
        
        # Mapping Payload from records
        refNum = (
            record.get("BuyOrder.ID") or
            record.get("id") or
            record.get("externalid") or
            record.get("refNum")
        )
        if refNum is not None:
            payload["refNum"] = str(refNum)
        
        orderedDate = (
            self._format_date(record.get("OrderDate")) or
            self._format_date(record.get("transaction_date")) or
            self._format_date(record.get("orderedDate"))
        )
        if orderedDate is not None:
            payload["orderedDate"] = str(orderedDate)
        
        arrivalDate = (
            self._format_date(record.get("expectedDeliveryDate")) or
            self._format_date(record.get("created_at")) or
            self._format_date(record.get("arrivalDate"))
        )   
        if arrivalDate is not None:
            payload["arrivalDate"] = str(arrivalDate)
        
        vendorID = (
            record.get("SupplierRemoteId") or
            record.get("supplier_remoteId") or
            record.get("vendorID")
        )
        if vendorID is not None:
            payload["vendorID"] = str(vendorID)
        
        # Map shopID (from record or config using export_buyOrder_shopid flag)
        shop_id = record.get("shopID") or self.config.get("buyorders_shop_id")
        if not shop_id:
            raise ValueError("shopID is required but not found in record or config (buyorders_shop_id)")
        
        payload["shopID"] = str(shop_id)
        
        # Optional fields (truthy check for strings)
        for field in ["shipInstructions", "stockInstructions"]:
            value = record.get(field)
            if value:
                payload[field] = value
        
        # Optional fields (explicit None check for numbers that can be 0)
        for field in ["shipCost", "otherCost", "discount"]:
            value = record.get(field)
            if value is not None:
                payload[field] = value
        
        payload["_line_items"] = line_items
        return payload
    
    def upsert_record(self, record: dict, context: dict) -> Tuple[Optional[str], bool, dict]:
        """Send the Order and OrderLines to the API and return the result."""
        state_updates = {}
        
        if not record:
            return None, False, state_updates
        
        line_items = record.pop("_line_items", [])
        
        try:
            self.logger.info(
                f"Making request to endpoint='{self.endpoint}' with method: 'POST' "
                f"and payload= {record}"
            )
            response = self.request_api(
                "POST",
                endpoint=self.endpoint,
                request_data=record
            )
            
            # Log response details
            self.logger.info(f"Response status code: {response.status_code}")
            try:
                response_data = response.json()
                self.logger.info(f"Full API response: {json.dumps(response_data, default=str)}")
            except Exception as json_err:
                self.logger.warning(f"Could not parse response as JSON: {json_err}")
                self.logger.info(f"Response text: {response.text[:1000]}")
                response_data = {}
            
            order_data = response_data.get("Order", {})
            order_id = order_data.get("orderID")
            
            if not order_id:
                order_id = response_data.get("orderID") or response_data.get("id")
            
            if not order_id:
                LOGGER.error(f"No orderID returned from API. Full response: {json.dumps(response_data, default=str)}")
                state_updates["error"] = "No orderID in API response"
                state_updates["api_response"] = response_data
                return None, False, state_updates
            
            self.logger.info(f"Order created successfully with orderID: {order_id}")
            
            if line_items:
                self.logger.info(f"Processing {len(line_items)} OrderLine(s) for orderID: {order_id}")
                lines_success = 0
                lines_failed = 0
                
                for idx, line_item in enumerate(line_items):
                    try:
                        # Lightspeed API expects all OrderLine fields as strings (per documentation)
                        order_line_payload = {
                            "orderID": str(order_id),
                            "quantity": str(line_item.get("quantity")),
                            "itemID": str(line_item.get("productId") or line_item.get("product_remoteId")),
                        }
                        
                        field_mappings = {
                            "price": line_item.get("OptiplySupplierProductPrice") or line_item.get("price"),
                            "originalPrice": line_item.get("originalPrice"),
                            "numReceived": line_item.get("numReceived"),
                            "vendorCost": line_item.get("vendorCost"),
                        }
                        
                        for field, value in field_mappings.items():
                            if value is not None:
                                order_line_payload[field] = str(value)
                        
                        self.logger.info(
                            f"Making request to endpoint='/OrderLine.json' with method: 'POST' "
                            f"and payload= {order_line_payload} "
                            f"(OrderLine {idx + 1}/{len(line_items)})"
                        )
                        line_response = self.request_api(
                            "POST",
                            endpoint="/OrderLine.json",
                            request_data=order_line_payload
                        )
                        line_response_data = line_response.json()
                        order_line_id = line_response_data.get("OrderLine", {}).get("orderLineID")
                        
                        if order_line_id:
                            self.logger.info(f"OrderLine {idx + 1} created successfully with orderLineID: {order_line_id}")
                            lines_success += 1
                        else:
                            LOGGER.warning(f"OrderLine {idx + 1} created but no orderLineID in response: {line_response_data}")
                            lines_success += 1
                            
                    except Exception as line_error:
                        import traceback
                        LOGGER.error(f"Error creating OrderLine {idx + 1}: {line_error}")
                        LOGGER.error(f"Error type: {type(line_error).__name__}")
                        LOGGER.error(f"Line item data: {json.dumps(line_item, default=str)}")
                        LOGGER.error(f"OrderLine payload: {json.dumps(order_line_payload, default=str)}")
                        
                        # Try to get API error response if available
                        if hasattr(line_error, 'response') and line_error.response is not None:
                            try:
                                error_data = line_error.response.json()
                                LOGGER.error(f"API Error Response: {json.dumps(error_data)}")
                            except:
                                LOGGER.error(f"API Error Response (text): {line_error.response.text[:1000]}")
                        
                        LOGGER.error(f"Traceback: {traceback.format_exc()}")
                        lines_failed += 1
                        continue
                
                self.logger.info(
                    f"OrderLines processing complete for orderID {order_id}: "
                    f"{lines_success} succeeded, {lines_failed} failed"
                )
                
                if lines_failed > 0:
                    LOGGER.warning(
                        f"Order {order_id} created but {lines_failed} OrderLine(s) failed. "
                        f"Order may need manual line addition."
                    )
            
            return str(order_id), True, state_updates
            
        except Exception as e:
            import traceback
            LOGGER.error("=" * 80)
            LOGGER.error(f"ERROR UPSERTING ORDER")
            LOGGER.error("=" * 80)
            LOGGER.error(f"Error: {e}")
            LOGGER.error(f"Error type: {type(e).__name__}")
            
            # Log the payload that caused the error
            try:
                LOGGER.error(f"Payload that caused error: {json.dumps(record, default=str)}")
            except:
                LOGGER.error(f"Payload (repr): {repr(record)}")
            
            # Try to get API error response if available
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    LOGGER.error(f"API Error Response: {json.dumps(error_data)}")
                    state_updates["api_error_response"] = error_data
                except:
                    LOGGER.error(f"API Error Response (text): {e.response.text[:1000]}")
                    state_updates["api_error_response"] = e.response.text[:1000]
            
            LOGGER.error(f"Full Traceback: {traceback.format_exc()}")
            LOGGER.error("=" * 80)
            
            state_updates["error"] = str(e)
            state_updates["error_type"] = type(e).__name__
            return None, False, state_updates


class FallbackSink(LightspeedRSeriesSink):
    """Fallback sink for streams that don't match any specific sink."""
    
    name = "fallback"
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record
    
    def upsert_record(self, record: dict, context: dict) -> Tuple[str, bool, dict]:
        LOGGER.warning(f"FallbackSink: No specific sink found for stream. Record: {record}")
        state_updates = {}
        return None, False, state_updates