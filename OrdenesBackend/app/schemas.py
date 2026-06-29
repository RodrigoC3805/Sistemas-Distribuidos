from pydantic import BaseModel
from typing import List

class ProductItem(BaseModel):
    code: str
    name: str
    quantity: int

class OrderRequest(BaseModel):
    items: List[ProductItem]