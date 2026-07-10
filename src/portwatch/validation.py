from __future__ import annotations

from collections import Counter
from datetime import date

from portwatch.models import TradeFlow


class DataValidationError(ValueError):
    """Raised when a batch violates a semantic data contract."""


def validate_trade_flows(
    flows: list[TradeFlow],
    *,
    expected_month: date | None = None,
    expected_port_code: str | None = None,
    expected_commodity_code: str | None = None,
) -> None:
    if not flows:
        raise DataValidationError("Census response contained no trade-flow records")

    duplicate_keys = [
        key for key, count in Counter(flow.natural_key for flow in flows).items() if count > 1
    ]
    if duplicate_keys:
        raise DataValidationError(f"duplicate trade-flow keys detected: {duplicate_keys[:3]}")

    for flow in flows:
        if expected_month is not None and flow.month != expected_month:
            raise DataValidationError(
                f"unexpected month {flow.month}; expected {expected_month}"
            )
        if expected_port_code is not None and flow.port_code != expected_port_code:
            raise DataValidationError(
                f"unexpected port {flow.port_code}; expected {expected_port_code}"
            )
        if expected_commodity_code is not None and (
            flow.commodity_code != expected_commodity_code
        ):
            raise DataValidationError(
                f"unexpected commodity {flow.commodity_code}; "
                f"expected {expected_commodity_code}"
            )
        if flow.containerized_value_usd > flow.vessel_value_usd:
            raise DataValidationError(
                "containerized vessel value exceeds total vessel value for "
                f"{flow.natural_key}"
            )
        if flow.containerized_weight_kg > flow.vessel_weight_kg:
            raise DataValidationError(
                "containerized vessel weight exceeds total vessel weight for "
                f"{flow.natural_key}"
            )

