# 仅负责数据转换部分，由Firehose调用触发
# Firehose sink的方式，表头无法直接写入 S3，需后续用 Glue Crawler 或 Athena 等工具设置字段映射。
# Firehose prefix 路径中的时间戳只能是 UTC，无法直接用本地时区（如 Australia/Sydney）。

import base64
import json

def transform_record(record):
    payload = base64.b64decode(record["data"]).decode("utf-8")
    data = json.loads(payload)
    # 处理 days_since_prior
    days = data.get("days_since_prior")
    if days is None:
        days = 0
    elif days > 30:
        days = 30
    data["days_since_prior"] = days
    # 新增 eval_set d
    data["eval_set"] = "dummy streaming"
    # 调整字段顺序
    ordered = {
        "order_id": data.get("order_id"),
        "user_id": data.get("user_id"),
        "eval_set": data.get("eval_set"),
        "order_number": data.get("order_number"),
        "order_dow": data.get("order_dow"),
        "order_hour_of_day": data.get("order_hour_of_day"),
        "days_since_prior": data.get("days_since_prior"),
    }
    out = json.dumps(ordered) + "\n"
    return base64.b64encode(out.encode("utf-8")).decode("utf-8")

def lambda_handler(event, context):
    output = []
    for record in event["records"]:
        try:
            transformed_data = transform_record(record)
            output.append({
                "recordId": record["recordId"],
                "result": "Ok",
                "data": transformed_data
            })
        except Exception as e:
            output.append({
                "recordId": record["recordId"],
                "result": "ProcessingFailed",
                "data": record["data"]
            })
    print("Transformed records:", output)
    return {"records": output}