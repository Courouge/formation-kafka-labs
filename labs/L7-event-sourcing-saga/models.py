"""Modèles Pydantic miroirs des schémas Avro.

Ces modèles servent à valider et manipuler les events côté Python avant la
sérialisation Avro (et après désérialisation). Ils sont volontairement
identiques aux .avsc pour éviter les divergences silencieuses.

Pattern : on utilise des dataclasses Pydantic frozen pour respecter
l'immuabilité (cf. coding-style commun, T2 §2.2).
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict


# --- Constantes topics --------------------------------------------------------
TOPIC_COMMANDS = "orders.commands"
TOPIC_EVENTS = "orders.events"
TOPIC_SNAPSHOTS = "orders.snapshots"


class OrderStatus(str, Enum):
    CREATED = "CREATED"
    PAID = "PAID"
    RESERVED = "RESERVED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _new_event_id() -> str:
    return str(uuid.uuid4())


# --- Préambule commun ---------------------------------------------------------
class EventEnvelope(BaseModel):
    """Champs communs à tous les events. event_id = clé idempotence."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(default_factory=_new_event_id)
    order_id: str
    occurred_at: int = Field(default_factory=_now_ms)
    version: int


# --- Items ---
class OrderItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    sku: str
    quantity: int
    unit_price: float


# --- Commands ---
class PlaceOrder(BaseModel):
    """Command côté entrée du système (publiée sur orders.commands)."""

    model_config = ConfigDict(frozen=True)

    command_type: Literal["PlaceOrder"] = "PlaceOrder"
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    items: list[OrderItem]
    currency: str = "EUR"

    @property
    def total(self) -> float:
        return round(sum(i.quantity * i.unit_price for i in self.items), 2)


# --- Events (subset utile au lab : ajouter les autres au besoin) -------------
class OrderCreated(EventEnvelope):
    event_type: Literal["OrderCreated"] = "OrderCreated"
    customer_id: str
    items: list[OrderItem]
    total: float
    currency: str = "EUR"


class PaymentRequested(EventEnvelope):
    event_type: Literal["PaymentRequested"] = "PaymentRequested"
    amount: float
    currency: str = "EUR"


class PaymentCompleted(EventEnvelope):
    event_type: Literal["PaymentCompleted"] = "PaymentCompleted"
    transaction_id: str
    amount: float


class PaymentFailed(EventEnvelope):
    event_type: Literal["PaymentFailed"] = "PaymentFailed"
    reason: str


class StockReserved(EventEnvelope):
    event_type: Literal["StockReserved"] = "StockReserved"
    reservation_id: str


class StockOutOfStock(EventEnvelope):
    event_type: Literal["StockOutOfStock"] = "StockOutOfStock"
    missing_sku: str


class StockReleased(EventEnvelope):
    event_type: Literal["StockReleased"] = "StockReleased"
    reservation_id: str


class OrderShipped(EventEnvelope):
    event_type: Literal["OrderShipped"] = "OrderShipped"
    tracking_number: str
    carrier: str = "UPS"


class OrderDelivered(EventEnvelope):
    event_type: Literal["OrderDelivered"] = "OrderDelivered"
    delivered_at: int = Field(default_factory=_now_ms)


class OrderCancelled(EventEnvelope):
    event_type: Literal["OrderCancelled"] = "OrderCancelled"
    reason: str


# --- État agrégé (résultat du fold) ------------------------------------------
class OrderState(BaseModel):
    """Vue courante d'un Order, dérivée par fold sur les events.

    Immutabilité : chaque transition retourne un NOUVEL OrderState.
    """

    model_config = ConfigDict(frozen=True)

    order_id: str
    version: int = 0
    status: OrderStatus = OrderStatus.CREATED
    customer_id: str = ""
    total: float = 0.0
    currency: str = "EUR"
    transaction_id: Optional[str] = None
    reservation_id: Optional[str] = None
    tracking_number: Optional[str] = None
    cancel_reason: Optional[str] = None

    def with_(self, **kwargs: Any) -> "OrderState":
        return self.model_copy(update=kwargs)
