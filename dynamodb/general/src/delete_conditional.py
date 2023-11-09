#!/usr/bin/env python3

# Test deleting entry for no-entry, condition-failed and successful-delete:


import logging
import boto3
import botocore.errorfactory

dynamodb = boto3.client("dynamodb")
dynamodbResource = boto3.resource("dynamodb")

def main():
    table = dynamodbResource.Table("zbynek_aws_exp_delete_conditional")

    #try:
    #    dynamodb.delete_table(TableName=table.table_name)
    #except Exception as ex:
    #    logging.error(ex)
    #    pass
    #table.wait_until_not_exists()

    try:
        dynamodb.create_table(
            TableName=table.table_name,
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

    for keycounter in range(0, 2):
        table.put_item(
            Item={
                "id": "hello",
                "keycounter": keycounter,
            },
        )

    response = table.delete_item(
        Key={
            "id": "hello",
            "keycounter": 0,
        },
        ReturnValues='ALL_OLD',
    )
    assert response.get('Attributes') is not None

    response = table.delete_item(
        Key={
            "id": "hello",
            "keycounter": 2,
        },
        ReturnValues='ALL_OLD',
    )
    assert response.get('Attributes') is None

    try:
        response = table.delete_item(
            Key={
                "id": "hello",
                "keycounter": 1,
            },
            ConditionExpression="attribute_exists(not_existent)",
            ReturnValues='ALL_OLD',
            ReturnValuesOnConditionCheckFailure='ALL_OLD',
        )
        assert False
    except dynamodb.exceptions.ConditionalCheckFailedException as ex:
        #assert response.get('Attributes') is not None
        pass

    try:
        response = table.delete_item(
            Key={
                "id": "hello",
                "keycounter": 2,
            },
            ReturnValues='ALL_OLD',
            ReturnValuesOnConditionCheckFailure='ALL_OLD',
            ConditionExpression="attribute_exists(not_existent)",
        )
    except dynamodb.exceptions.ConditionalCheckFailedException as ex:
        assert response.get('Attributes') is None



main()
