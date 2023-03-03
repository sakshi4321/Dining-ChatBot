import json

"""
This sample demonstrates an implementation of the Lex Code Hook Interface
in order to serve a sample bot which manages orders for flowers.
Bot, Intent, and Slot models which are compatible with this sample can be found in the Lex Console
as part of the 'OrderFlowers' template.

For instructions on how to set up and test this bot, as well as additional samples,
visit the Lex Getting Started documentation http://docs.aws.amazon.com/lex/latest/dg/getting-started.html.
"""
import math
import dateutil.parser
import datetime
import time
import os
import json
import logging
import boto3
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
from botocore.exceptions import ClientError



""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


""" --- SQS Queue --- """    

# def create_queue(name, attributes=None):
#     """
#     Creates an Amazon SQS queue.

#     :param name: The name of the queue. This is part of the URL assigned to the queue.
#     :param attributes: The attributes of the queue, such as maximum message size or
#                       whether it's a FIFO queue.
#     :return: A Queue object that contains metadata about the queue and that can be used
#              to perform queue operations like sending and receiving messages.
#     """
#     if not attributes:
#         attributes = {}

#     try:
#         queue = sqs.create_queue(
#             QueueName=name,
#             Attributes=attributes
#         )
#         logger.info("Created queue '%s' with URL=%s", name, queue.url)
#     except ClientError as error:
#         logger.exception("Couldn't create queue named '%s'.", name)
#         raise error
#     else:
#         return queue

def get_queue(name):
    """
    Gets an SQS queue by name.

    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
    try:
        queue = sqs.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def send_message(queue, message_body, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response

""" --- Helper Functions --- """


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def validate_booking(location, cuisine_type, booking_time, no_people, phone, date):

    if location is not None and location.lower() != "manhattan":
        return build_validation_result(False, 'location', "Please enter a valid location")
        
    
    cuisine_types = ['chinese', 'indian', 'mexican','thai','american','italian', 'french', 'greek', 'mediterranean']
    if cuisine_type is not None and cuisine_type.lower() not in cuisine_types:
        return build_validation_result(False,
                                       'CuisineType',
                                       'We do not have data for those restaurants, would you like to go to a different cuisine?')
    

    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'BookingDate', 'I did not understand that, on what date would you like to make the reservation?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False, 'BookingDate', 'You can make the reservations from today onwards.On what day would you like to make the reservation?')

    if booking_time is not None:
        if len(booking_time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'BookingTime', None)

        hour, minute = booking_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'BookingTime', None)

        if hour < 12 or hour > 23:
            # Outside of business hours
            return build_validation_result(False, 'BookingTime', 'You can make a reservation from 12 noon to 11 p m. Can you specify a time during this range?')
    
    if no_people is not None:
        no_people=int(no_people)
        if no_people > 20:
            return build_validation_result(False, 'no_people', "You can make a reservation for 1 person and upto 20 people, for more than 20 please contact the restaurant directly. Can you specify a number in the range?  ")
    
    if phone is not None:
        if len(str(phone)) != 10:
            return build_validation_result(False, 'phone','Please enter a valid phone number')


    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """
sqs = boto3.resource('sqs')

def greeting_intent(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi, how can I help?'}
        }
    }

def thankyou_intent(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Thank you'}
        }
    }

def diningsuggestion_intent(intent_request):
    """
    Performs dialog management and fulfillment for ordering flowers.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """
    email_addrs = get_slots(intent_request)['email']
    location = get_slots(intent_request)["location"]
    cuisine_type = get_slots(intent_request)["CuisineType"]
    no_people = get_slots(intent_request)["people"]
    date = get_slots(intent_request)["BookingDate"]
    booking_time = get_slots(intent_request)["BookingTime"]
    phone = get_slots(intent_request)["phone"]
    source = intent_request['invocationSource']   # souce?invocationSource?

    # try: 
    #     create_queue("restaurant_request")
    # except:
    #     print("already exists")

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = get_slots(intent_request)

        validation_result = validate_booking(location, cuisine_type, booking_time, no_people, phone, date)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        # Pass the price of the flowers back through session attributes to be used in various prompts defined
        # on the bot model.
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        # if cuisine_type is not None:
        #     output_session_attributes['Price'] = len(cuisine_type) * 5  # Elegant pricing model

        return delegate(output_session_attributes, get_slots(intent_request))
    sqs = boto3.client('sqs')
    # queue = sqs.get_queue(QueueName='restaurant_request')
    msg = {'location': location,"cuisine": cuisine_type, "phone": phone, "email":email_addrs, "date": date, "time":booking_time, "num_ppl":no_people}
    response = sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/375818523005/restaurant_request',MessageBody=json.dumps(msg))
     
    # sqs.send_message(
    #     QueueUrl="https://sqs.us-east-1.amazonaws.com/375818523005/restaurant_requeste",
    #     MessageBody=current_time
    # )
   
    

    # Order the flowers, and rely on the goodbye message of the bot to define the message to the end user.
    # In a real bot, this would likely involve a call to a backend service.
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thanks! your restaurant suggestions will be sent over shortly'})


""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name  == 'greeting_intent':   #OrderFlowers?
        return greeting_intent(intent_request)
    if intent_name == 'Recommendation':   #OrderFlowers?
        return diningsuggestion_intent(intent_request)
    if intent_name  == 'thanks_intent':   #OrderFlowers?
        return thankyou_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
