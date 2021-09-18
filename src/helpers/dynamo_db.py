import boto3
from boto3.dynamodb.conditions import Key

class DynamoHelper:
    """
    Args:
        table_name: helps dynamo class instantiation to isolate to a single table
    """
    def __init__(self, table_name):
        self.dynamo = boto3.resource('dynamodb','us-east-1')
        self.table_name = self.dynamo.Table(table_name)

    def get_table(self):
        return self.dynamo.Table(self.table_name)

    def get_item(self,table_name,partition_key,partition_value):
        try:
            response = self.table_name.get_item(
                Key={
                    partition_key: partition_value
                    }
            )
            if_zero={'times_used':0, partition_key:partition_value}
            items = response.get("Item",if_zero)
            return items
        except Exception as e:
            raise e

    def put_item(self,table_name,partition_key,partition_value,times_used,updated_on):
        try:
            response = self.table_name.put_item(
                Item={
                    partition_key:partition_value,
                    'times_used':times_used,
                    'updated_on':updated_on
                }
            )
            return response    
        except:
            raise Exception('Unable to create item in DynamoDB table')       

    def update_item(self,table_name,partition_key,partition_value,times_used,updated_on):
        try:
            response = self.table_name.update_item(
            Key={
                partition_key:partition_value,
            },
            UpdateExpression="set times_used=:r, set updated_on=:s",
            ExpressionAttributeValues={
                ':r':times_used,
                ':s':updated_on
            },
            ReturnValues="UPDATED_NEW"
        )
            return response
        except:
            raise Exception('Unable to update item in DynamoDB table')                


class DynamoHelperWithExamples:
    """
    Args:
        table_name: helps dynamo class instantiation to isolate to a single table
    """
    def __init__(self, table_name):
        dynamo = boto3.resource('dynamodb')
        self.table = dynamo.Table(table_name)


    # in the future you can put your helper functions here like so
    def delete_item(self, table_name, partition_key, partition_value, sort_key=None, sort_value=None):
        try:
            item = {partition_key: partition_value}

            if sort_key and sort_value:
                item.update({sort_key: sort_value})

            response = self.table.delete_item(
                Key=item,
                ReturnValues='ALL_OLD'
            )

            return response
        except Exception as e:
            raise e


    def put_item(self, table_name, item):
        try:
            self.table.put_item(
                Item=item
            )
        except Exception as e:
            raise e


    def query_table(self, table_name, partition_key, partition_value, sort_key=None, sort_value=None,
        sort_key_operator=None, sort_order="asc", secondary_index=None, filter_key=None, filter_value=None):
        """
        Perform a query operation on the table.
        Can specify partition_key (col name) and its value to be filtered.
        """
        scan_index_forward = True
        if sort_order == "desc":
            scan_index_forward = False

        query_exp = Key(partition_key).eq(partition_value)

        if sort_key and sort_value:
            if sort_key_operator == "greater_than":
                query_exp = Key(partition_key).eq(partition_value) & Key(sort_key).gt(sort_value)
            else:
                query_exp = Key(partition_key).eq(partition_value) & Key(sort_key).eq(sort_value)

        params = {
            "KeyConditionExpression": query_exp,
            "ScanIndexForward": scan_index_forward
        }

        if secondary_index:
            params.update({"IndexName":secondary_index})

        if filter_key is not None and filter_value is not None:
            filtering_exp = Key(filter_key).eq(filter_value)
            params.update({"FilterExpression":filtering_exp})

        try:
            response = self.table.query(**params)
            return response
        except Exception as e:
            raise e


    def scan_table(self, table_name, proj_exp=None, exp_attr_names=None, filter_key=None, filter_value=None):
        """
        Perform a scan operation on the table.
        Can specify filter_key (col name) and its value to be filtered.
        """
        try:
            if filter_key is None and filter_value is None and proj_exp is not None:
                response = self.table.scan(
                    ProjectionExpression=proj_exp,
                    ExpressionAttributeNames=exp_attr_names
                )
                return response
            elif filter_key is not None and filter_value is not None:
                filtering_exp = Key(filter_key).eq(filter_value)
                response = self.table.scan(ProjectionExpression=proj_exp, FilterExpression=filtering_exp)
                return response
            else:
                response = self.table.scan()
                return response
        except Exception as e:
            raise e
