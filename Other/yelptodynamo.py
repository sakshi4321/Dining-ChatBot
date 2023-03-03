    
# Import the modules
import boto3
from botocore.vendored import requests
import json
import time
import calendar
import datetime
from decimal import *
# Define a business ID
# business_id = '4AErMBEoNzbk7Q8g45kKaQ'
unix_time = 1546047836

API_KEY = 'ViYxOHxzCLRrO-zEzJIkak-LPmRl6Mw0uTu2EjcwHk_yKMgRZiX1U5yIu_QGR4TRGsZ8ma5hPjLwvDh_rdeD9-HQ1Nes7BCaQad5ACgGjCxYAQDsJaAYP9g9ENbjY3Yx'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': 'bearer %s' % API_KEY}

def create_table():
    client = boto3.client('dynamodb', region_name='us-east-1')
    try:
        resp = client.create_table(
            TableName="restaurants",
            # Declare your Primary Key in the KeySchema argument
            KeySchema=[
                {
                    "AttributeName": "id",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "rating",
                    "KeyType": "RANGE"
                }
            ],
            # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
            AttributeDefinitions=[
                {
                    "AttributeName": "id",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "name",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "category",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "coordinates",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "no_of_reviews",
                    "AttributeType": "N"
                },
                {
                    "AttributeName": "rating",
                    "AttributeType": "N"
                },
                {
                    "AttributeName": "location",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "zipcode",
                    "AttributeType": "S"
                }

            ],
            # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
            # You can control read and write capacity independently.
            ProvisionedThroughput={
                "ReadCapacityUnits": 25,
                "WriteCapacityUnits": 25
            }
        )
        print("Table created successfully!")
    except Exception as e:
        print("Error creating table:")
        print(e)
def process_data(data, alias):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('restaurants')
    
    final=[]
    #cats=['name', 'categories','location','price','phone']
    j=0
    #print(data)
    with table.batch_writer() as writer:
        for i in data:
            # print(i)
            
            j+=1
            temp={}
            es_temp={}
            temp['id']=i['id']
            temp['name']=i['name']
            temp['category']=alias
            temp['coordinates']=str([i['coordinates']['latitude'],i['coordinates']['longitude']])
            # temp['coordinates_longitude']=
            temp['rating']=Decimal(str(i['rating']))
            temp['no_of_reviews']=Decimal(str(i['review_count']))
            temp['location']=i['location']['display_address']
            temp['zipcode']=i['location']['zip_code']
            current_time = datetime.datetime.now()
        
            time_stamp = current_time.timestamp()
            temp['time_stamp'] = str(time_stamp)
            writer.put_item(Item=temp)
            if j==25:
                time.sleep(1)
                
        
def scrape_data():
    cuisines=[ "Mediterranean", "Greek", "French", "mexican","italian", "indian","thai", "chinese","american", "french"]
    #cuisines=[ "mexican",italian", "indian","thai", "chinese","american", "french"]
    for c in cuisines:
        #print(c)
        for i in range(50,1000, 50):
            print(i)
            
            PARAMETERS = {'term': c+' ' +'restaurants',
                        'limit': 50,
                        'offset': i,
                        'location': 'Manhatten'}
    
    
            # Make a request to the Yelp API
            response = requests.get(url = ENDPOINT,
                                    params = PARAMETERS,
                                    headers = HEADERS)
    
            
            business_data = response.json()
            data=business_data['businesses']
            process_data(data, c)
        #     break
        
        


            

def lambda_handler(event, context):
    # TODO implement
    #create_table()
    scrape_data()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
