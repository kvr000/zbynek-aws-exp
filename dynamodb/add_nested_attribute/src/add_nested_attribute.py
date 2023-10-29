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
    table = dynamodbResource.Table("zbynek_aws_exp_add_nested_attribute")

    #try:
    #    dynamodb.delete_table(TableName="zbynek_aws_exp_add_nested_attribute")
    #except Exception as ex:
    #    logging.error(ex)
    #    pass
    #table.wait_until_not_exists()

    try:
        dynamodb.create_table(
            TableName="zbynek_aws_exp_add_nested_attribute",
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
            "dynamics": {},
        },
    )
    table.update_item(
        Key={
            "id": "hello",
        },
        UpdateExpression="SET dynamics.rootvalue = :zero",
        ExpressionAttributeValues={
            ":zero": 0,
        },
    )
    table.update_item(
        Key={
            "id": "hello",
        },
        UpdateExpression="SET dynamics.nested1 = :nested",
            ConditionExpression="attribute_not_exists(dynamics.nested1)",
        ExpressionAttributeValues={
            ":nested": { "nestedvalue": 0 },
        },
    )
    try:
        table.update_item(
            Key={
                "id": "hello",
            },
            UpdateExpression="SET dynamics.nested1 = :nested",
            ConditionExpression="attribute_not_exists(dynamics.nested1)",
            ExpressionAttributeValues={
                ":nested": { "nestedvalue": 1 },
            },
        )
        raise("Expected exception for update on existing value")
    except dynamodb.exceptions.ConditionalCheckFailedException as ex:
        pass
    table.update_item(
        Key={
            "id": "hello",
        },
        UpdateExpression="SET dynamics.nested1.nestedvalue = dynamics.nested1.nestedvalue + :three",
        ConditionExpression="attribute_exists(dynamics.nested1)",
        ExpressionAttributeValues={
            ":three": 3,
        },
    )

    table.update_item(
        Key={
            "id": "hello",
        },
        UpdateExpression="SET dynamics.#name = :nested",
            ConditionExpression="attribute_not_exists(dynamics.#name)",
        ExpressionAttributeNames={
            '#name': "false#true",
        },
        ExpressionAttributeValues={
            ":nested": { "nestedvalue": 0 },
        },
    )
    table.update_item(
        Key={
            "id": "hello",
        },
        UpdateExpression="SET dynamics.#name.nestedvalue = dynamics.#name.nestedvalue + :four",
        ConditionExpression="attribute_exists(dynamics.#name)",
        ExpressionAttributeNames={
            '#name': "false#true",
        },
        ExpressionAttributeValues={
            ":four": 4,
        },
    )

    item = table.get_item(
        Key={
            "id": "hello",
        },
    )['Item']
    assert item['dynamics']['nested1']['nestedvalue'] == 3
    assert item['dynamics']['false#true']['nestedvalue'] == 4


main()
