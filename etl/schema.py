from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable

@dataclass
class FieldSpec:
    name: str
    required: bool = False
    synonyms: List[str] = field(default_factory=list)
    transform: Optional[Callable] = None

@dataclass
class EntitySpec:
    name: str
    fields: List[FieldSpec]
    required_min: List[str]

    @property
    def synonym_map(self) -> Dict[str, str]:
        mapping = {}
        for f in self.fields:
            mapping[f.name.lower()] = f.name
            for s in f.synonyms:
                mapping[s.lower()] = f.name
        return mapping

# Sales
SALES = EntitySpec(
    name="sales",
    fields=[
        FieldSpec("date", required=True, synonyms=["sale_date", "datetime", "order_date", "txn_date", "timestamp"]),
        FieldSpec("product_name", synonyms=["item", "service", "sku", "description", "item_name", "service_name"]),
        FieldSpec("quantity", synonyms=["qty", "units", "count", "pcs"]),
        FieldSpec("unit_price", synonyms=["price", "rate", "unit_rate", "sale_price"]),
        FieldSpec("amount", synonyms=["total", "gross", "net", "revenue", "line_total"]),
        FieldSpec("customer", synonyms=["client", "buyer", "patient"]),
        FieldSpec("staff", synonyms=["stylist", "salesperson", "technician", "agent"]),
        FieldSpec("category", synonyms=["type", "dept", "service_type", "product_category"]),
        FieldSpec("branch", synonyms=["location", "shop", "store"]),
    ],
    required_min=["date", "amount"],
)

# Expenses
EXPENSES = EntitySpec(
    name="expenses",
    fields=[
        FieldSpec("date", required=True, synonyms=["expense_date", "txn_date", "timestamp"]),
        FieldSpec("expense_category", required=True, synonyms=["category", "head", "account", "type"]),
        FieldSpec("amount", required=True, synonyms=["total", "value", "cost", "paid"]),
        FieldSpec("vendor", synonyms=["supplier", "payee"]),
        FieldSpec("branch", synonyms=["location", "shop", "store"]),
        FieldSpec("notes", synonyms=["memo", "remark", "description"]),
    ],
    required_min=["date", "expense_category", "amount"],
)

# Appointments (Calendar)
APPOINTMENTS = EntitySpec(
    name="appointments",
    fields=[
        FieldSpec("start_time", required=True, synonyms=["start", "start_datetime", "start_date", "appointment_start", "begin"]),
        FieldSpec("end_time", synonyms=["end", "end_datetime", "end_date", "appointment_end", "finish"]),
        FieldSpec("service", synonyms=["service_name", "treatment", "subject", "title", "event"]),
        FieldSpec("staff", synonyms=["stylist", "technician", "consultant"]),
        FieldSpec("customer", synonyms=["client", "patient", "name", "guest"]),
        FieldSpec("branch", synonyms=["location", "shop", "store"]),
        FieldSpec("notes", synonyms=["description", "remarks", "memo"]),
        FieldSpec("amount", synonyms=["price", "charge", "fee"]),
    ],
    required_min=["start_time"],
)

ENTITY_SPECS = {
    "sales": SALES,
    "expenses": EXPENSES,
    "appointments": APPOINTMENTS,
}
