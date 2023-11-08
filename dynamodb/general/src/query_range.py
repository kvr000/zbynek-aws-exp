#!/usr/bin/env python3

# Test adding nested attribute in DynamoDb table, using conditional update:
# 
# Expected structure (nested1 and false#true added dynamically if not existed yet):
#
# ```
# {
#     'id': 'hello",
#     'dynamics': {
#         'nested1': {
#             'nestedvalue': 3,
#         },
#         'false#true': {
#             'nestedvalue': 4,
#         },
#     }
# }
# ```


import logging
import boto3
import botocore.errorfactory

dynamodb = boto3.client("dynamodb")
dynamodbResource = boto3.resource("dynamodb")

def main():
    table = dynamodbResource.Table("zbynek_aws_exp_query_range")

    #try:
    #    dynamodb.delete_table(TableName="zbynek_aws_exp_query_range")
    #except Exception as ex:
    #    logging.error(ex)
    #    pass
    #table.wait_until_not_exists()

    try:
        dynamodb.create_table(
            TableName="zbynek_aws_exp_query_range",
            KeySchema=[
                {
                    "AttributeName": "id",
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": "event_time",
                    "KeyType": "RANGE",
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "id",
                    "AttributeType": "S",
                },
                {
                    "AttributeName": "event_time",
                    "AttributeType": "S",
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    except dynamodb.exceptions.ResourceInUseException as ex:
        pass
    table.wait_until_exists()

    for time in [ "2001-01-01T00:00:00", "2001-01-02T00:00:00", "2001-01-03T00:00:00", "2001-01-05T00:00:00", ]:
        table.put_item(
            Item={
                "id": "hello",
                "event_time": time,
            },
        )

    items = []
    lastEvaluated = None
    while True:
        response = table.query(
            KeyConditionExpression="id = :id AND event_time BETWEEN :start AND :end_excl",
            ExpressionAttributeValues={
                ":id": "hello",
                ":start": "2001-01-02T00:00:00",
                ":end_excl": "2001-01-05T00:00:00",
            },
            **({} if lastEvaluated is None else {'ExclusiveStartKey': {"S": lastEvaluated}}),
        )
        items.extend(response['Items'])
        lastEvaluated = response.get('LastEvaluatedKey')
        if lastEvaluated is None:
            break

    # Unfortunately, DynamoDb is missing inclusive-exclusive BETWEEN operator, so we have to check and potentially cut the last element manually:
    if items[-1]['event_time'] == "2001-01-05T00:00:00":
        items.pop()

    assert len(items) == 2
    assert items[0]['event_time'] == "2001-01-02T00:00:00"
    assert items[1]['event_time'] == "2001-01-03T00:00:00"


main()
