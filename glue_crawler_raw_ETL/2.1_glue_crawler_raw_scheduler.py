# lambda_trigger_crawlers.py
# This Lambda is triggered by EventBridge and starts one or more Glue Crawlers based on event payload

import boto3
import os

glue = boto3.client("glue")

def lambda_handler(event, context):
    crawlers = os.getenv("CRAWLER_NAMES", "").split(",")
    if not crawlers:
        return {"status": "No crawlers configured in environment"}

    results = []
    for crawler_name in crawlers:
        try:
            glue.start_crawler(Name=crawler_name.strip())
            results.append({"crawler": crawler_name.strip(), "status": "started"})
        except Exception as e:
            results.append({"crawler": crawler_name.strip(), "status": "error", "message": str(e)})

    return {"results": results}
