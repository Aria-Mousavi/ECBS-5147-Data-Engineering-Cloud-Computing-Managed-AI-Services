import datetime
import json

import boto3
import requests

# Must match your existing bucket
S3_WIKI_BUCKET = "aria-wikidata"


def lambda_handler(event, context):
    """
    Lambda handler for Wikipedia pageviews ETL pipeline.

    Optional event:
      {"date": "YYYY-MM-DD"}
    If not provided, defaults to 21 days ago (UTC).
    """
    # 1) Determine date
    date_str = event.get("date") if isinstance(event, dict) else None
    if date_str:
        date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    else:
        date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=21)

    # 2) Extract: call pageviews top endpoint
    project = "en.wikipedia"
    access = "all-access"
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{project}/{access}/{date.strftime('%Y/%m/%d')}"
    resp = requests.get(url, headers={"User-Agent": "curl/7.68.0"})

    if resp.status_code != 200:
        raise Exception(f"Wikipedia API error: {resp.status_code} - {resp.text}")

    # 3) Transform: JSON Lines with required fields
    top_pages = resp.json()["items"][0]["articles"]
    current_time = datetime.datetime.now(datetime.timezone.utc)

    json_lines = ""
    for page in top_pages:
        record = {
            "title": page["article"],
            "views": int(page["views"]),
            "rank": int(page["rank"]),
            "date": date.strftime("%Y-%m-%d"),
            "retrieved_at": current_time.replace(tzinfo=None).isoformat(),
        }
        json_lines += json.dumps(record) + "\n"

    # 4) Load: upload to S3
    s3 = boto3.client("s3")
    s3_key = f"raw-views/raw-views-{date.strftime('%Y-%m-%d')}.json"
    s3.put_object(Bucket=S3_WIKI_BUCKET, Key=s3_key, Body=json_lines)

    return {
        "statusCode": 200,
        "body": f"Uploaded {len(top_pages)} records to s3://{S3_WIKI_BUCKET}/{s3_key}",
    }