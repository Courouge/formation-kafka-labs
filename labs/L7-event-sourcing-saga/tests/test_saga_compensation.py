"""Test compensation : forcer un OutOfStock et vérifier l'annulation.

On utilise SKU-003 dont le stock initial est 0 dans stock_service.py.
"""

import pytest

from .conftest import collect_events_for_order, publish_place_order


@pytest.mark.integration
def test_compensation_out_of_stock(producer, event_consumer):
    order_id = publish_place_order(
        producer,
        customer_id="cust-comp",
        items=[{"sku": "SKU-003", "quantity": 1, "unit_price": 30.0}],
    )

    events = collect_events_for_order(
        event_consumer,
        order_id,
        expected_types=["OrderCreated", "PaymentCompleted", "StockOutOfStock", "OrderCancelled"],
        timeout_s=30,
    )

    has_out_of_stock = any("missing_sku" in e for e in events)
    has_cancel = any(("reason" in e) and ("missing_sku" not in e) and ("items" not in e)
                     and ("transaction_id" not in e) for e in events)

    assert has_out_of_stock, "expected StockOutOfStock"
    assert has_cancel, "expected OrderCancelled after compensation"
