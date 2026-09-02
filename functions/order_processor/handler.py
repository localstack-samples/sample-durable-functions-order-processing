import json
import os

import boto3
from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration, StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

orders_table = dynamodb.Table(os.environ["ORDERS_TABLE"])
notifications_topic_arn = os.environ["NOTIFICATIONS_TOPIC"]

PAYMENT_SUCCEEDS_ON_ATTEMPT = 3

payment_retry_strategy = create_retry_strategy(
    RetryStrategyConfig(
        max_attempts=5,
        initial_delay=Duration.from_seconds(2),
        backoff_rate=2.0,
    )
)


class PaymentGatewayTimeout(Exception):
    pass


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    order = context.step(validate_order(event), name="validate-order")
    context.step(record_order(order), name="record-order")

    charge = context.step(
        charge_payment(order),
        name="charge-payment",
        config=StepConfig(retry_strategy=payment_retry_strategy),
    )

    callback = context.create_callback(
        name="payment-confirmation",
        config=CallbackConfig(timeout=Duration.from_hours(1)),
    )
    context.step(
        publish_callback_id(order["order_id"], callback.callback_id),
        name="publish-callback-id",
    )
    confirmation = json.loads(callback.result())

    context.parallel(
        [
            lambda ctx: ctx.step(send_notification(order, "EMAIL")),
            lambda ctx: ctx.step(send_notification(order, "SMS")),
        ],
        name="notify-customer",
    )

    context.step(mark_fulfilled(order["order_id"], charge["charge_id"]), name="mark-fulfilled")
    return {
        "order_id": order["order_id"],
        "status": "FULFILLED",
        "charge_id": charge["charge_id"],
        "payment_attempts": charge["attempts"],
        "confirmation": confirmation,
    }


@durable_step
def validate_order(step: StepContext, event: dict) -> dict:
    for field in ("order_id", "customer_email", "items"):
        if not event.get(field):
            raise ValueError(f"order is missing required field: {field}")
    total_cents = sum(item["quantity"] * item["unit_price_cents"] for item in event["items"])
    step.logger.info(f"Order {event['order_id']} validated, total {total_cents} cents")
    return {
        "order_id": event["order_id"],
        "customer_email": event["customer_email"],
        "total_cents": total_cents,
    }


@durable_step
def record_order(step: StepContext, order: dict) -> dict:
    orders_table.put_item(
        Item={
            "order_id": order["order_id"],
            "customer_email": order["customer_email"],
            "total_cents": order["total_cents"],
            "order_status": "PROCESSING",
        }
    )
    step.logger.info(f"Order {order['order_id']} recorded")
    return {"order_id": order["order_id"]}


@durable_step
def charge_payment(step: StepContext, order: dict) -> dict:
    attempt = next_payment_attempt(order["order_id"])
    if attempt < PAYMENT_SUCCEEDS_ON_ATTEMPT:
        raise PaymentGatewayTimeout(f"payment gateway timed out on attempt {attempt}")
    charge_id = f"ch_{order['order_id'].lower()}"
    step.logger.info(f"Charged {order['total_cents']} cents as {charge_id} on attempt {attempt}")
    return {"charge_id": charge_id, "attempts": attempt}


def next_payment_attempt(order_id: str) -> int:
    response = orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="ADD payment_attempts :one",
        ExpressionAttributeValues={":one": 1},
        ReturnValues="UPDATED_NEW",
    )
    return int(response["Attributes"]["payment_attempts"])


@durable_step
def publish_callback_id(step: StepContext, order_id: str, callback_id: str) -> dict:
    orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="SET callback_id = :cb, order_status = :status",
        ExpressionAttributeValues={":cb": callback_id, ":status": "AWAITING_CONFIRMATION"},
    )
    step.logger.info(f"Order {order_id} waits for payment confirmation")
    return {"order_id": order_id}


@durable_step
def send_notification(step: StepContext, order: dict, channel: str) -> dict:
    sns.publish(
        TopicArn=notifications_topic_arn,
        Subject=f"Order {order['order_id']} confirmed",
        Message=f"[{channel}] Your order {order['order_id']} is confirmed and paid.",
        MessageAttributes={"channel": {"DataType": "String", "StringValue": channel}},
    )
    step.logger.info(f"Notification sent via {channel}")
    return {"channel": channel}


@durable_step
def mark_fulfilled(step: StepContext, order_id: str, charge_id: str) -> dict:
    orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="SET order_status = :status, charge_id = :charge",
        ExpressionAttributeValues={":status": "FULFILLED", ":charge": charge_id},
    )
    step.logger.info(f"Order {order_id} fulfilled")
    return {"order_id": order_id}
