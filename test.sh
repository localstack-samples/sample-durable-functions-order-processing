#!/bin/bash
# Run one order through the durable workflow the way the README describes:
# 1. the payment step fails twice and succeeds on the third attempt
# 2. the execution parks at the payment confirmation callback
# 3. optionally LocalStack restarts while it waits (RESTART_CMD, e.g. "lstk restart")
# 4. the callback succeeds and the execution finishes with the order fulfilled,
#    each completed step recorded once and both notifications delivered
set -euo pipefail

FUNCTION=${FUNCTION:-order-processor}
TABLE=${TABLE:-orders}
QUEUE=${QUEUE:-order-notifications-sink}
RESTART_CMD=${RESTART_CMD:-}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-60}

RUN_ID=$(date +%s)
ORDER_ID="ORD-CI-$RUN_ID"
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

jq --arg id "$ORDER_ID" '.order_id = $id' events/order.json > "$WORK_DIR/order.json"
expected_total=$(jq '[.items[] | .quantity * .unit_price_cents] | add' events/order.json)

order_attr() {
  lstk aws dynamodb get-item --table-name "$TABLE" --key "{\"order_id\": {\"S\": \"$ORDER_ID\"}}" \
    --query "Item.$1" --output text
}

execution_status() {
  lstk aws lambda get-durable-execution --durable-execution-arn "$ARN" --query Status --output text
}

ARN=$(lstk aws lambda invoke --function-name "$FUNCTION" --qualifier '$LATEST' \
  --invocation-type Event --durable-execution-name "order-ci-$RUN_ID" \
  --payload "fileb://$WORK_DIR/order.json" "$WORK_DIR/response.json" \
  --query DurableExecutionArn --output text)
echo "Started execution $ARN"

order_status=""
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  order_status=$(order_attr order_status.S 2>/dev/null || true)
  [ "$order_status" = "AWAITING_CONFIRMATION" ] && break
  echo "Attempt $attempt/$MAX_ATTEMPTS: order status '$order_status'"
  sleep 2
done
if [ "$order_status" != "AWAITING_CONFIRMATION" ]; then
  echo "Test failed: order $ORDER_ID never reached AWAITING_CONFIRMATION"
  lstk aws lambda get-durable-execution --durable-execution-arn "$ARN" || true
  exit 1
fi
echo "PASS: order parked at the payment confirmation callback"

payment_attempts=$(order_attr payment_attempts.N)
total=$(order_attr total_cents.N)
if [ "$payment_attempts" != "3" ] || [ "$total" != "$expected_total" ]; then
  echo "Test failed: expected 3 payment attempts and total $expected_total, got $payment_attempts and $total"
  exit 1
fi
echo "PASS: payment succeeded on attempt 3, total $total cents"

status=$(execution_status)
if [ "$status" != "RUNNING" ]; then
  echo "Test failed: execution is $status while waiting for the callback"
  exit 1
fi
echo "PASS: execution is RUNNING while it waits"

if [ -n "$RESTART_CMD" ]; then
  echo "Restarting LocalStack with: $RESTART_CMD"
  $RESTART_CMD
  status=""
  for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    status=$(execution_status 2>/dev/null || true)
    [ -n "$status" ] && break
    echo "Attempt $attempt/$MAX_ATTEMPTS: waiting for the execution to be restored"
    sleep 2
  done
  if [ "$status" != "RUNNING" ]; then
    echo "Test failed: execution is $status after the restart"
    exit 1
  fi
  echo "PASS: execution survived the restart"
fi

CALLBACK_ID=$(order_attr callback_id.S)
lstk aws lambda send-durable-execution-callback-success --callback-id "$CALLBACK_ID" \
  --cli-binary-format raw-in-base64-out \
  --result '{"payment_status": "captured", "gateway_ref": "pg-91c7"}'
echo "Sent the payment confirmation"

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  status=$(execution_status)
  case "$status" in
    SUCCEEDED) break ;;
    FAILED|TIMED_OUT|STOPPED)
      echo "Test failed: execution ended as $status"
      lstk aws lambda get-durable-execution --durable-execution-arn "$ARN"
      exit 1 ;;
  esac
  echo "Attempt $attempt/$MAX_ATTEMPTS: execution $status"
  sleep 2
done
if [ "$status" != "SUCCEEDED" ]; then
  echo "Test failed: execution did not finish, last status $status"
  exit 1
fi
echo "PASS: execution SUCCEEDED"

result=$(lstk aws lambda get-durable-execution --durable-execution-arn "$ARN" --query Result --output text)
if ! echo "$result" | jq -e --arg id "$ORDER_ID" '.order_id == $id and .status == "FULFILLED"
    and .payment_attempts == 3 and .charge_id == ("ch_" + ($id | ascii_downcase))
    and .confirmation.payment_status == "captured"' > /dev/null; then
  echo "Test failed: unexpected result $result"
  exit 1
fi
echo "PASS: result reports the order fulfilled with the confirmation"

history=$(lstk aws lambda get-durable-execution-history --durable-execution-arn "$ARN" --output json)
for step in validate-order record-order charge-payment publish-callback-id mark-fulfilled; do
  count=$(echo "$history" | jq --arg s "$step" '[.Events[] | select(.EventType == "StepSucceeded" and .Name == $s)] | length')
  if [ "$count" != "1" ]; then
    echo "Test failed: step $step succeeded $count times, expected once"
    echo "$history" | jq -c '[.Events[] | [.EventType, .Name]]'
    exit 1
  fi
done
if ! echo "$history" | jq -e 'any(.Events[]; .EventType == "CallbackSucceeded")' > /dev/null; then
  echo "Test failed: history has no CallbackSucceeded event"
  exit 1
fi
echo "PASS: history records each step once and the callback"

if [ "$(order_attr order_status.S)" != "FULFILLED" ] || [ "$(order_attr charge_id.S)" != "ch_$(echo "$ORDER_ID" | tr '[:upper:]' '[:lower:]')" ]; then
  echo "Test failed: order $ORDER_ID is not marked FULFILLED with its charge in DynamoDB"
  exit 1
fi
echo "PASS: order marked FULFILLED in DynamoDB"

QUEUE_URL=$(lstk aws sqs get-queue-url --queue-name "$QUEUE" --query QueueUrl --output text)
channels=""
for attempt in $(seq 1 10); do
  bodies=$(lstk aws sqs receive-message --queue-url "$QUEUE_URL" --max-number-of-messages 10 \
    --wait-time-seconds 2 --query 'Messages[].Body' --output json)
  channels="$channels $(echo "$bodies" | jq -r --arg id "$ORDER_ID" '.[]? | select(contains($id)) | capture("^\\[(?<c>[A-Z]+)\\]").c')"
  [[ "$channels" == *EMAIL* && "$channels" == *SMS* ]] && break
done
if [[ "$channels" != *EMAIL* || "$channels" != *SMS* ]]; then
  echo "Test failed: expected EMAIL and SMS notifications for $ORDER_ID, got:$channels"
  exit 1
fi
echo "PASS: EMAIL and SMS notifications reached the SQS sink"

echo "Test passed: order $ORDER_ID went through the durable workflow"
