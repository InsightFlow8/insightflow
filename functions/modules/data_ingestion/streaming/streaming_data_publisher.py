import os
import json
import random
import boto3

# Dummy ORDERS table data publisher for AWS Kinesis

KINESIS_STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "test1")

def random_order(order_id):
    return {
        "order_id": order_id,
        "user_id": random.randint(1, 100000),
        "order_number": random.randint(1, 100),
        "order_dow": random.randint(0, 6),
        "order_hour_of_day": random.randint(0, 23),
        "days_since_prior": random.choice([random.randint(1, 10000), None])
    }
 
def lambda_handler(event, context):
    kinesis = boto3.client("kinesis")
    records = []
    for i in range(10):  # 生成10条
        order = random_order(order_id=1000000 + i)
        records.append({
            "Data": json.dumps(order),
            "PartitionKey": str(order["user_id"])
        })
    # 批量推送
    response = kinesis.put_records(
        StreamName=KINESIS_STREAM_NAME,
        Records=records
    )
    print("PutRecords response:", response)
    print(f'Send records to {KINESIS_STREAM_NAME} successfully.')
    return {"status": "success", "records_sent": len(records)}

# 本地测试
if __name__ == "__main__":
    # os.environ["KINESIS_STREAM_NAME"] = "test1"
    lambda_handler({}, None)