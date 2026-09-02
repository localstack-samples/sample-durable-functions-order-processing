from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_sqs as sqs,
)
from constructs import Construct


class OrderProcessingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        orders_table = dynamodb.Table(
            self,
            "OrdersTable",
            table_name="orders",
            partition_key=dynamodb.Attribute(name="order_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        notifications_topic = sns.Topic(
            self, "NotificationsTopic", topic_name="order-notifications"
        )
        notifications_sink = sqs.Queue(
            self, "NotificationsSink", queue_name="order-notifications-sink"
        )
        notifications_topic.add_subscription(
            subscriptions.SqsSubscription(notifications_sink, raw_message_delivery=True)
        )

        order_processor = lambda_.Function(
            self,
            "OrderProcessor",
            function_name="order-processor",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("functions/order_processor/build"),
            timeout=Duration.seconds(120),
            memory_size=256,
            durable_config=lambda_.DurableConfig(
                execution_timeout=Duration.hours(1),
                retention_period=Duration.days(7),
            ),
            environment={
                "ORDERS_TABLE": orders_table.table_name,
                "NOTIFICATIONS_TOPIC": notifications_topic.topic_arn,
            },
        )

        orders_table.grant_read_write_data(order_processor)
        notifications_topic.grant_publish(order_processor)

        CfnOutput(self, "OrderProcessorName", value=order_processor.function_name)
        CfnOutput(self, "OrdersTableName", value=orders_table.table_name)
        CfnOutput(self, "NotificationsSinkUrl", value=notifications_sink.queue_url)
