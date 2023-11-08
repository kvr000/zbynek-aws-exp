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
    table = dynamodbResource.Table("zbynek_aws_exp_null_check")

    #try:
    #    dynamodb.delete_table(TableName="zbynek_aws_exp_add_nested_attribute")
    #except Exception as ex:
    #    logging.error(ex)
    #    pass
    #table.wait_until_not_exists()

    try:
        dynamodb.create_table(
            TableName="zbynek_aws_exp_null_check",
            KeySchema=[
                {
                    "AttributeName": "id",
                    "KeyType": "HASH",
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "id",
                    "AttributeType": "S",
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    except dynamodb.exceptions.ResourceInUseException as ex:
        pass
    table.wait_until_exists()

    table.delete_item(
        Key={
            "id": "hello",
        },
    )
    table.put_item(
        Item={
            "id": "hello",
            "thenull": None,
        },
    )

    table.update_item(
        Key={
            "id": "hello",
        },
        ConditionExpression="thenull = :null",
        UpdateExpression="SET thenull = :null",
        ExpressionAttributeValues={
            ":null": None,
        },
    )

    try:
        table.update_item(
            Key={
                "id": "hello",
            },
            UpdateExpression="SET updated = :one",
            ConditionExpression="attribute_not_exists(thenull)",
            ExpressionAttributeValues={
                ":one": 1,
            },
        )
        raise("Expected exception for update on existing value")
    except dynamodb.exceptions.ConditionalCheckFailedException as ex:
        pass

    table.update_item(
        Key={
            "id": "hello",
        },
        UpdateExpression="SET updated = :two",
        ConditionExpression="attribute_not_exists(thenull) or thenull = :null",
        ExpressionAttributeValues={
            ":null": None,
            ":two": 2,
        },
    )

    item = table.get_item(
        Key={
            "id": "hello",
        },
    )['Item']
    assert item['updated'] == 2


main()
