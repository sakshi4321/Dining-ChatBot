import boto3
import sys
import os
import json
import boto3
#import logger
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from boto3.dynamodb.conditions import Key
from botocore.vendored import requests

#Send email for SES
def send_email(sender_email, message):
    msg = MIMEMultipart()
    msg["Subject"] = "Restaurant recommedation for you!"
    msg["From"] = "smk8939@nyu.edu"
    msg["To"] = "smk8939@nyu.edu"

    # Set message body
    body = MIMEText(message)
    msg.attach(body)


    # Convert message to string and send
    ses_client = boto3.client("ses", region_name="us-east-1")
    print("t1")
    response = ses_client.send_raw_email(
        Source="smk8939@nyu.edu",
        Destinations=["smk8939@nyu.edu"],
        RawMessage={"Data": msg.as_string()}
    )
    #print(response)
    return response



#Get Restaurant from Elastic Search
def elastic_search_id(cuisine):
    print("ES1")
    headers = {'content-type': 'application/json'}
    esUrl = 'https://search-restaurants-zggsreqybwzmor3yxdgzwv7tpy.us-east-1.es.amazonaws.com/_search?q=category:'+cuisine+ '&size=5'
    print(esUrl)
    esResponse = requests.get(esUrl, auth=("DiningES", "DiningES@123"), headers=headers)
    print(esResponse)
    #logger.debug("esResponse: {}".format(esResponse.text))
    data = json.loads(esResponse.content.decode('utf-8'))
    print(data)
    #logger.info("data: {}".format(data))
    try:
        esData = data["hits"]["hits"]
        #print(esData)
        return esData
        #logger.info("esData: {}".format(esData))
    except KeyError:
        es_id="10tnq8x2qI7ix7VqVP0rMw"
        return es_id


#Get Data from DynamoDB
def query_data_with_sort(es_id):
    
    print("test3")
    table_name = "restaurants"
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression=Key('id').eq(es_id)
            
        )
    except botocore.exceptions.ParamValidationError as e:
        print(f"ParamValidationError: {e}")
        return None
    # print(type(response["Items"]))
    # print(response["Items"])
    if len(response["Items"]) > 0:
        return response['Items'][0]
    else:
        return []
    
  
# Receive message from SQS
def receive_message():
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.receive_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/375818523005/restaurant_request",
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10,
    )

    # if message in response["Messages"][0] is None:
    #     return None,None,None
    
    for message in response.get("Messages", []):
        #print("test")
        message_body = message["Body"]
        print(message)
        # print(f"Receipt Handle: {message['ReceiptHandle']}")
        indi_msg = json.loads(message['Body'])
        cuisine = indi_msg['cuisine']
        
        customer_phone = indi_msg['phone']
        customer_email = indi_msg['email']
        no_ppl = indi_msg["num_ppl"]
        time = indi_msg["time"]
        date = indi_msg["date"]
        Message_to_send = "Hello! Here are some "+ cuisine +" restaurant suggestions for " +str(no_ppl)+ " people, for "+ str(date)+ "\n"+ " at "+ str(time)  + " Enjoy your meal!"
        #print(message)
        es_data=elastic_search_id(cuisine)
        #print(es_data)
        j=0
        for i in range(0,len(es_data)):
            
            es_id = es_data[i]['_source']["id"]
            print(es_id)
            item_db = query_data_with_sort(es_id)
            if len(item_db) >0:
                print(j)
                j+=1
                if 'phone' in item_db.keys():
                    restaurant_phone = str(item_db['phone'])
                else:
                    restaurant_phone = 'NA'
                addr = str(item_db['location'])
                Message_to_send += "\n"+ str(j) +".  "+ str(item_db['name']) + "at " + addr
        
        
        #print(Message_to_send)
        sqs_client.delete_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/375818523005/restaurant_request",
            ReceiptHandle=message['ReceiptHandle']
        )
        response = send_email(customer_email, Message_to_send)
        #print("Msg Sent")
    return Message_to_send, customer_phone, customer_email


def lambda_handler(event, context):
    try:
     

        message_to_send, customer_phone, customer_email = receive_message()
        #print(customer_email)
        
        
        
       
    except:
        print('Error', sys.exc_info()[0])