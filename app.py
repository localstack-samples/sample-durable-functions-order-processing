import aws_cdk as cdk

from stacks.order_processing_stack import OrderProcessingStack

app = cdk.App()
OrderProcessingStack(app, "OrderProcessingStack")
app.synth()
