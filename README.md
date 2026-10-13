# Order Processing with Lambda Durable Functions on LocalStack

A moderately complex order fulfillment workflow built as a single AWS Lambda
Durable Function, developed and tested entirely on LocalStack.

The workflow, in one durable handler:

1. Validate the order and compute the total.
2. Record the order in DynamoDB.
3. Charge a flaky payment gateway. The step fails twice and succeeds on the
   third attempt through the SDK retry strategy.
4. Create a callback and park the execution until the payment provider
   confirms the charge.
5. Notify the customer over email and SMS in parallel branches (SNS, with an
   SQS queue subscribed as a test sink).
6. Mark the order fulfilled in DynamoDB.

Because the execution is durable, you can restart LocalStack while the
workflow waits for the payment confirmation. The execution resumes from its
last checkpoint. Completed steps do not run again.

## Prerequisites

- [`lstk`](https://docs.localstack.cloud/aws/getting-started/installation/) with a LocalStack Auth Token (durable executions are a Pro feature)
- [Docker](https://docs.docker.com/get-docker/)
- Node.js and the AWS CDK CLI (>= 2.1139.0)
- Python 3.13
- AWS CLI with botocore >= 1.43 (the durable Lambda operations are new)

## Deploy

```bash
# package the handler with the durable execution SDK (the LocalStack runtime
# images do not bundle it)
./build.sh

# create the CDK virtualenv
python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt

# start LocalStack with persistence, then deploy
lstk start --persist
lstk cdk bootstrap
lstk cdk deploy
```

## Run an order

```bash
ARN=$(awslocal lambda invoke \
  --function-name order-processor \
  --qualifier '$LATEST' \
  --invocation-type Event \
  --durable-execution-name order-1001 \
  --payload fileb://events/order.json \
  response.json --query DurableExecutionArn --output text)

awslocal lambda get-durable-execution --durable-execution-arn "$ARN"
```

Watch the payment step retry, then park at the callback:

```bash
awslocal dynamodb get-item --table-name orders \
  --key '{"order_id":{"S":"ORD-1001"}}'
```

Optional: restart LocalStack here (`lstk restart`) to prove the execution
survives a crash.

Confirm the payment to resume the workflow:

```bash
CB=$(awslocal dynamodb get-item --table-name orders \
  --key '{"order_id":{"S":"ORD-1001"}}' \
  --query 'Item.callback_id.S' --output text)

awslocal lambda send-durable-execution-callback-success \
  --callback-id "$CB" \
  --result '{"payment_status": "captured", "gateway_ref": "pg-91c7"}'
```

Inspect the finished execution:

```bash
awslocal lambda get-durable-execution --durable-execution-arn "$ARN" --query Result
awslocal lambda get-durable-execution-history --durable-execution-arn "$ARN" \
  --query 'Events[].EventType'
awslocal sqs receive-message \
  --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/order-notifications-sink \
  --max-number-of-messages 10 --query 'Messages[].Body'
```

## Test

`test.sh` runs a fresh order through the steps above and checks the payment
attempts, the parked callback, the final result, the execution history, the
DynamoDB record and both notifications. Set `RESTART_CMD` to restart
LocalStack while the execution waits for the callback:

```bash
RESTART_CMD="lstk restart" ./test.sh
```

CI runs the same test on every pull request.

## Layout

```
app.py                              CDK app entry point
stacks/order_processing_stack.py    DynamoDB + SNS + SQS + durable Lambda
functions/order_processor/          handler and its requirements
build.sh                            packages the handler with the durable SDK
events/order.json                   sample order payload
test.sh                             end-to-end test, run by CI
```
