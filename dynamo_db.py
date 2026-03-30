from fastapi import APIRouter, FastAPI, HTTPException
from pydantic import BaseModel
import boto3
from boto3.dynamodb.conditions import Key

# app = FastAPI()


router = APIRouter(prefix="/orders", tags=["Orders"])

# DynamoDB config
dynamodb = boto3.resource(
    "dynamodb",
    region_name="us-east-1"
)
table = dynamodb.Table("table-test")

# ---------------------------
# Pydantic Models
# ---------------------------

class Order(BaseModel):
    id: str
    order_id: str
    amount: int
    status: str

class UpdateOrder(BaseModel):
    amount: int | None = None
    status: str | None = None

# ---------------------------
# CREATE
# ---------------------------

@router.post("/orders")
def create_order(order: Order):
    table.put_item(Item=order.dict())
    return {"message": "Order created successfully"}

# ---------------------------
# READ (Single Order)
# ---------------------------

# @router.get("/orders/{id}")
# def get_order(id: str):

    
#     response = table.get_item(
#         Key={
#             "id": id
#             # "order_id": order_id
#         }
#     )


#     if "Item" not in response:
#         raise HTTPException(status_code=404, detail="Order not found")

#     return response["Item"]

# ---------------------------
# READ (All Orders of User)
# ---------------------------

@router.get("/orders/{user_id}")
def get_user_orders(user_id: str):
    response = table.query(
        KeyConditionExpression=Key("id").eq(user_id)
    )
    return response["Items"]

# ---------------------------
# UPDATE
# ---------------------------

@router.put("/orders/{user_id}/{order_id}")
def update_order(user_id: str, order_id: str, data: UpdateOrder):
    update_expr = []
    expr_attr_values = {}

    if data.amount is not None:
        update_expr.append("amount = :a")
        expr_attr_values[":a"] = data.amount

    if data.status is not None:
        update_expr.append("status = :s")
        expr_attr_values[":s"] = data.status

    if not update_expr:
        raise HTTPException(status_code=400, detail="Nothing to update")

    table.update_item(
        Key={
            "user_id": user_id,
            "order_id": order_id
        },
        UpdateExpression="SET " + ", ".join(update_expr),
        ExpressionAttributeValues=expr_attr_values
    )

    return {"message": "Order updated successfully"}

# ---------------------------
# DELETE
# ---------------------------

@router.delete("/orders/{user_id}/{order_id}")
def delete_order(user_id: str, order_id: str):
    table.delete_item(
        Key={
            "user_id": user_id,
            "order_id": order_id
        }
    )
    return {"message": "Order deleted successfully"}
