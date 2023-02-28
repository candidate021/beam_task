from typing import NamedTuple
from datetime import date

class TransactionRow(NamedTuple):
    timestamp: date
    origin: str
    destination: str
    transaction_amount: float