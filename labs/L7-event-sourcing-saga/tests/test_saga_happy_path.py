"""Test saga happy path : PlaceOrder -> OrderShipped.

Pré-requis : les 4 services doivent tourner (lancer dans 4 terminaux séparés
avant de lancer ce test) :
    python -m services.order_service
    python -m services.payment_service
    python -m services.stock_service
    python -m services.shipping_service
"""

import pytest

from .conftest import collect_events_for_order, publish_place_order


@pytest.mark.integration
def test_happy_path(producer, event_consumer):
    order_id = publish_place_order(
        producer,
        customer_id="cust-happy",
        items=[{"sku": "SKU-001", "quantity": 1, "unit_price": 50.0}],
    )

    events = collect_events_for_order(
        event_consumer,
        order_id,
        expected_types=["OrderCreated", "PaymentCompleted", "StockReserved", "OrderShipped"],
        timeout_s=30,
    )

    types_seen = []
    for e in events:
        if "items" in e:
            types_seen.append("OrderCreated")
        elif "transaction_id" in e:
            types_seen.append("PaymentCompleted")
        elif "reservation_id" in e and "reason" not in e:
            types_seen.append("StockReserved")
        elif "tracking_number" in e:
            types_seen.append("OrderShipped")

    assert "OrderCreated" in types_seen
    assert "PaymentCompleted" in types_seen
    assert "StockReserved" in types_seen
    assert "OrderShipped" in types_seen

    # L'ordre doit être respecté grâce au partitionnement par order_id
    idx = {t: i for i, t in enumerate(types_seen)}
    assert idx["OrderCreated"] < idx["PaymentCompleted"]
    assert idx["PaymentCompleted"] < idx["StockReserved"]
    assert idx["StockReserved"] < idx["OrderShipped"]
