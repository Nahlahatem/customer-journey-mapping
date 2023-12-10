Lowest_Price = 500
Medium_Price = 1000
Highest_Price = 1500

import time
from kafka import KafkaConsumer
import pandas as pd
from datetime import datetime
import json
from confluent_kafka import Producer
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import numpy as np
import uuid
from datetime import datetime, timedelta

customer_schema = {
    "type": "record",
    "name": "Customer",
    "fields": [
        {"name": "customer_id", "type": "int"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "start_date", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "phone", "type": "string"},
        {"name": "country", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "gender", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}
customer_schema_str = json.dumps(customer_schema)
schema_customer = avro.loads(customer_schema_str)
######################################################
product_schema = {
  "type": "record",
  "name": "Product",
  "fields": [
    {"name": "product_id", "type": "long"},
    {"name": "category_id", "type": "long"},
    {"name": "category_code", "type": "string"},
    {"name": "brand", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "main_category", "type": "string"},
    {"name": "sub_category", "type": "string"},
    {"name": "sub_category2", "type": "string"},
    {"name": "price_range", "type": "string"}
  ]
}

product_schema_str = json.dumps(product_schema)
schema_product = avro.loads(product_schema_str)
######################################################
time_schema = {
  "type": "record",
  "name": "TimeData",
  "fields": [
    {"name": "time_id", "type": "long"},
    {"name": "full_date", "type": "string"},
    {"name": "year", "type": "int"},
    {"name": "quarter", "type": "string"},
    {"name": "month", "type": "int"},
    {"name": "day_of_week", "type": "string"},
    {"name": "day_of_month", "type": "int"},
    {"name": "day_of_year", "type": "int"},
    {"name": "week_of_year", "type": "string"},
    {"name": "hour", "type": "int"}
  ]
}

time_schema_str = json.dumps(time_schema)
schema_time = avro.loads(time_schema_str)
######################################################
transaction_schema = {
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "customer_id", "type": "int"},
    {"name": "user_session", "type": "string"},
    {"name": "time_id", "type": "long"},
    {"name": "product_id", "type": "int"},
    {"name": "event_type", "type": "string"},
    {"name": "combined_Event", "type": "string"},
    {"name": "Revenue", "type": "double"}
  ]
}
transaction_schema_str = json.dumps(transaction_schema)
schema_transaction = avro.loads(transaction_schema_str)
######################################################
campign_schema = {
  "type": "record",
  "name": "campign",   
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "campion_1", "type": "int"},
    {"name": "campion_2", "type": "int"},
    {"name": "campion_3", "type": "int"},
    {"name": "campion_4", "type": "int"},
    {"name": "campion_5", "type": "int"},
    {"name": "used_discount", "type": "int"}
  ]
}
campign_schema_str = json.dumps(campign_schema)
schema_campign = avro.loads(campign_schema_str)
######################################################


bootstrap_servers = ['kafka:9092']
consumer = KafkaConsumer( bootstrap_servers=bootstrap_servers)
#consumer.topics()


def preprocess_null_values_dict(data, key, replace_with=None, drop_threshold=0.05):
    # Replace null values with a given word
    if replace_with is not None and key in data:
        data[key] = data.get(key, replace_with)
        return data

    # Drop key if its value is None and drop_threshold is satisfied
    if drop_threshold > 0 and key in data and data[key] is None:
        del data[key]
        return data

    return data


def add_uuid_to_dict(data, key_name):
    new_data = data.copy()
    new_data[key_name] = uuid.uuid4().hex
    return new_data



def add_datetime_info_to_dict(data):
    event_time = data['event_time'] 
    data['day_of_week'] = event_time.strftime('%A')
    data['Hour'] = event_time.hour
    data['event_date'] = event_time.date()

    # Calculate the date
    reference_date = datetime(1970, 1, 1)
    converted_date = reference_date + timedelta(days=data['start_date'])
    data['start_date'] = converted_date
    return data


def split_category( category):
    parts = category.split('.')
    main_category = parts[0] if len(parts) >= 1 else 'Other'
    sub_category = parts[1] if len(parts) >= 2 else 'Other'
    sub_category2 = parts[2] if len(parts) >= 3 else 'Other'
    return main_category, sub_category, sub_category2


def process_category_code(data):
    main_category, sub_category, sub_category2 = split_category(data['category_code'])
    combined_event = f"{data['event_type']}_{main_category}"
    
    
    data['main_category'] =  main_category
    data['sub_category'] =  sub_category
    data['sub_category2'] =  sub_category2
    data['combined_Event'] =  combined_event
    
    return  data 





############################################

def process_product_data(data, Lowest_Price, Medium_Price, Highest_Price):
    product = {
        'product_id': data['product_id'],
        'category_id': data['category_id'],
        'category_code': data['category_code'],
        'brand': data['brand'],
        'price': data['price'],
        'main_category': data['main_category'],
        'sub_category': data['sub_category'],
        'sub_category2': data['sub_category2']
    }

    if ((product['price']>= Lowest_Price) & (product['price']< Medium_Price)):
        price_range =  'low'
    elif ((product['price']>= Medium_Price) & (product['price']< Highest_Price)):
        price_range =  'medium'
    else:
        price_range =  'high'
    product['price_range'] = price_range

    return product

def process_transaction_data(data):
    transaction = {
        'transaction_id': data['transaction_id'],
        'customer_id': data['user_id'],
        'user_session': data['user_session'],
        'time_id': data['event_date'].year * 1000000 + data['event_date'].month * 10000 + data['event_date'].day*100+data['Hour'],
        'product_id': data['product_id'],
        # 'event_date': data['event_date'].isoformat(),
        'event_type': data['event_type'],
        'combined_Event': data['combined_Event']
        
        # 'day_of_week': data['day_of_week'],
        # 'Hour': data['Hour']
    }

    if transaction['event_type'] == 'purchase':
        revenue = data['price']
    else:
        revenue = 0
    transaction['Revenue'] = revenue

    return transaction


def process_Customer_data(data):
    Customer = {
        'customer_id': data['user_id'],
        'first_name': data['first_name'],
        'last_name': data['last_name'],
        'start_date': data['start_date'].isoformat(),
        'email': data['email'],
        'phone': data['phone_number'],
        'country': data['country'],
        'city': data['city'],
        'gender': 'Male' if data['gender'] == 'M' else 'Female',
        'age': data['age']
    }

    return Customer

def process_campign_data(data):

    campign = {
        'transaction_id': data['transaction_id'],
        'campion_1': data['campion_1'],
        'campion_2': data['campion_2'],
        'campion_3': data['campion_3'],
        'campion_4': data['campion_4'],
        'campion_5': data['campion_5'],
        'used_discount': data['used_discount']
       
    }

    return campign

def process_time_data(data):
    if 1 <= data['event_date'].month <= 3:
        quarter = 'Q1'
    elif 4 <= data['event_date'].month <= 6:
        quarter = 'Q2'
    elif 7 <= data['event_date'].month <= 9:
        quarter = 'Q3'
    else:
        quarter = 'Q4'

    Time = {
        'time_id':  data['event_date'].year * 1000000 + data['event_date'].month * 10000 + data['event_date'].day*100+data['Hour'],
        'full_date': data['event_date'].isoformat(),
        'year': data['event_date'].year,
        'quarter': quarter,
        'month': data['event_date'].month,
        'day_of_week': data['event_date'].strftime('%A') ,
        'day_of_month': data['event_date'].day,
        'day_of_year': data['event_date'].timetuple().tm_yday,
        'week_of_year': data['event_date'].strftime('%U'),
        'hour': data['Hour']
    }

    return Time


def process_ecommerce_data(data,Lowest_Price = 0,Medium_Price = 500,Highest_Price = 1000,Session_duration = 2,Event_duration = 40):
    
    

    data['event_time'] = datetime.utcfromtimestamp(data['event_time'] / 1000000)
    print(data['start_date'])


    # Apply preprocessing steps
    data = preprocess_null_values_dict(data, key='user_session')
    data = preprocess_null_values_dict(data, key='category_code', replace_with='other')
    data = preprocess_null_values_dict(data, key='brand', replace_with='other')

    data = add_uuid_to_dict(data, 'transaction_id')

    data = add_datetime_info_to_dict(data)


    data = process_category_code(data)

# Dimentions Of Warehouse
    product = process_product_data(data, Lowest_Price  , Medium_Price, Highest_Price)
    Customer = process_Customer_data(data)
    Time = process_time_data(data)

# Fact Table Of the warehouse
    Transaction = process_transaction_data(data)
    campign = process_campign_data(data)
    
    return Transaction, product,Customer,Time,campign





topicName = 'Ecom.public.transaction'
# Initialize consumer variable
consumer = KafkaConsumer (topicName , auto_offset_reset='earliest',  bootstrap_servers = bootstrap_servers, group_id='StreamingConnector')

# Initialize Kafka producer
producer_conf = {
    'bootstrap.servers': 'kafka:9092',  # Your Kafka broker address
}


producer_customer = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=schema_customer)

producer_product = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=schema_product)

producer_time = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=schema_time)

producer_transaction = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=schema_transaction)


producer_campign = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=schema_campign)


# producer = Producer(producer_conf)

Transaction_topic = "Transaction"
product_topic = "ProductINFO"
Customer_topic = "CustomerINFO"
Time_topic = "TimeINFO"
campign_topic = "campignData"



for msg in consumer:
    

    data = json.loads(msg.value)
    new_dict =  data['payload']
    
    
    Transaction, product,Customer,Time,campign = process_ecommerce_data(new_dict)
    # json_Transaction,json_product,json_Customer,json_time = json.dumps(Transaction), json.dumps(product), json.dumps(Customer), json.dumps(Time)
    
    # Send the JSON data to Kafka topic
   
    producer_product.produce(topic=product_topic, value=product,value_schema=schema_product)
    producer_customer.produce(topic=Customer_topic, value=Customer,value_schema=schema_customer)
    producer_time.produce(topic=Time_topic, value=Time,value_schema=schema_time)
    producer_transaction.produce(topic=Transaction_topic, value=Transaction,value_schema=schema_transaction)
    producer_campign.produce(topic=campign_topic, value=campign,value_schema=schema_campign)

consumer.close()    
# producer.flush()