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
    table = dynamodbResource.Table("zbynek_aws_exp_query_continuous")

    #try:
    #    dynamodb.delete_table(TableName="zbynek_aws_exp_query_continuous")
    #except Exception as ex:
    #    logging.error(ex)
    #    pass
    #table.wait_until_not_exists()

    try:
        dynamodb.create_table(
            TableName="zbynek_aws_exp_query_continuous",
            KeySchema=[
                {
                    "AttributeName": "id",
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": "keycounter",
                    "KeyType": "RANGE",
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "id",
                    "AttributeType": "S",
                },
                {
                    "AttributeName": "keycounter",
                    "AttributeType": "N",
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    except dynamodb.exceptions.ResourceInUseException as ex:
        pass
    table.wait_until_exists()

    if table.get_item(Key={"id": "hello", "keycounter": 999}).get("Item") is None:
        for keycounter in range(0, 1000):
            table.put_item(
                Item={
                    "id": "hello",
                    "keycounter": keycounter,
                    "value": "10-char-s\n" * 1000,
                },
            )

    items = []
    lastEvaluated = None
    runs = 0
    while True:
        response = table.query(
            KeyConditionExpression="id = :id AND keycounter BETWEEN :start AND :end_excl",
            ExpressionAttributeValues={
                ":id": "hello",
                ":start": 0,
                ":end_excl": 10000,
            },
            **({} if lastEvaluated is None else {'ExclusiveStartKey': lastEvaluated}),
        )
        items.extend(response['Items'])
        lastEvaluated = response.get('LastEvaluatedKey')
        runs = runs + 1
        if lastEvaluated is None:
            break

    # Unfortunately, DynamoDb is missing inclusive-exclusive BETWEEN operator, so we have to check and potentially cut the last element manually:
    if items[-1]['keycounter'] == 10000:
        items.pop()

    assert(runs > 1)

    assert len(items) == 1000
    assert items[0]['keycounter'] == 0
    assert items[1]['keycounter'] == 1


main()
