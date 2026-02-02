"""LightspeedRSeries target class."""

from __future__ import annotations

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_lightspeedrseries.sinks import (
    BuyOrders,
)


class TargetLightspeedRSeries(TargetHotglue):
    
   
    name = "target-lightspeedrseries"
    
    SINK_TYPES = [BuyOrders]
    MAX_PARALLELISM = 1
    
    config_jsonschema = th.PropertiesList(
        th.Property( "access_token", th.StringType, required=False),
        th.Property( "refresh_token", th.StringType, required=True),
        th.Property( "client_secret", th.StringType, required=True),
        th.Property( "client_id", th.StringType, required=True),
        th.Property( "expires_in", th.IntegerType, required=False),
        th.Property( "account_ids", th.StringType, required=True)
    ).to_dict()


if __name__ == "__main__":
    TargetLightspeedRSeries.cli()
