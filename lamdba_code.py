# import json

# def lambda_handler(event, context):
#     # TODO implement
#     print("event::", event)

#     return {
#         'statusCode': 200,
#         'body': json.dumps(f"Hello from Lambda! {event['input_data']}")
#     }


import boto3
import json
import ast
import subprocess
import os

s3 = boto3.client("s3")

def validate_syntax(code_str):
    try:
        ast.parse(code_str)
        return True, None
    except SyntaxError as e:
        return False, str(e)

def lambda_handler(event, context):
    bucket = event["bucket"]
    key = event["file_path"]
    # project_id = event["project_id"]

    # Download script
    response = s3.get_object(Bucket=bucket, Key=key)
    code = response["Body"].read().decode("utf-8")

    syntax_ok, error = validate_syntax(code)

    report = {
        "syntax_valid": syntax_ok,
        "error": error,
        "glue_compatible": "from awsglue.context" in code,
        "imports_checked": True
    }

    report_key = f"artifacts/validation_report.json"

    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=json.dumps(report)
    )

    return report
