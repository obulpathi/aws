from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
import time

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

table = dynamodb.Table('test-table')

for i in range(5):
    time.sleep(1)
    info = "The new stuff" + str(i)

    response = table.put_item(
       Item={
            'id': i,
            'info': info,
        }
    )

    print("PutItem succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEncoder))
