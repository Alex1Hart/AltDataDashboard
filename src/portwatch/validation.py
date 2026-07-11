from __future__ import annotations

from collections import Counter
from datetime import date

from portwatch.models import PortMetricName, PortOperation, TradeFlow


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
            raise DataValidationError(f"unexpected month {flow.month}; expected {expected_month}")
        if expected_port_code is not None and flow.port_code != expected_port_code:
            raise DataValidationError(
                f"unexpected port {flow.port_code}; expected {expected_port_code}"
            )
        if expected_commodity_code is not None and (flow.commodity_code != expected_commodity_code):
            raise DataValidationError(
                f"unexpected commodity {flow.commodity_code}; expected {expected_commodity_code}"
            )
        if flow.containerized_value_usd > flow.vessel_value_usd:
            raise DataValidationError(
                f"containerized vessel value exceeds total vessel value for {flow.natural_key}"
            )
        if flow.containerized_weight_kg > flow.vessel_weight_kg:
            raise DataValidationError(
                f"containerized vessel weight exceeds total vessel weight for {flow.natural_key}"
            )


def validate_port_operations(operations: list[PortOperation]) -> None:
    if not operations:
        raise DataValidationError("port operations response contained no records")

    duplicate_keys = [
        key
        for key, count in Counter(operation.natural_key for operation in operations).items()
        if count > 1
    ]
    if duplicate_keys:
        raise DataValidationError(f"duplicate port-operation keys detected: {duplicate_keys[:3]}")

    expected_metrics = set(PortMetricName)
    observed_metrics = {operation.metric for operation in operations}
    if observed_metrics != expected_metrics:
        missing = expected_metrics - observed_metrics
        raise DataValidationError(f"missing required port metrics: {sorted(missing)}")

    values = {operation.metric: operation.value for operation in operations}
    if (
        abs(
            values[PortMetricName.TOTAL_LOADED_TEU]
            - values[PortMetricName.LOADED_IMPORT_TEU]
            - values[PortMetricName.LOADED_EXPORT_TEU]
        )
        > 1
    ):
        raise DataValidationError("total loaded TEU does not reconcile to imports plus exports")
    if (
        abs(
            values[PortMetricName.TOTAL_TEU]
            - values[PortMetricName.TOTAL_LOADED_TEU]
            - values[PortMetricName.TOTAL_EMPTY_TEU]
        )
        > 1
    ):
        raise DataValidationError("total TEU does not reconcile to loaded plus empty")
