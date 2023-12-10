import requests
import pandas as pd
import time
from datetime import datetime, timedelta

from kafka import KafkaConsumer
import pandas as pd
import json
import uuid

# Define your Power BI API endpoint and API key
api_url = 'https://api.powerbi.com/beta/635448dd-5608-444b-b9de-b06a0f8fb81f/datasets/a364e3a8-5768-4222-a4d8-5f453f15cb2e/rows?redirectedFromSignup=1&experience=power-bi&key=gZgfWszfP1fkoM24nSxTDP0u4B2HNSug7CGXCthBG0K9%2BxeCC%2BheXNdssPfCJdEtpjJU5rZaJZk1dDWrkk3v7w%3D%3D'
# Read data from CSV (assuming your CSV has columns named 'Column1' and 'Column2')


# Function to send data to Power BI
def send_data_to_power_bi(row):
    # print(row['payload'])
    data = row['payload']

    
    # event_time_microseconds = data['event_time']
    # event_time_seconds = event_time_microseconds / 1000000  # Convert to seconds
    # event_datetime = datetime.utcfromtimestamp(event_time_seconds)

    # # Convert event_datetime to a string if needed
    # data['event_time'] = str(event_datetime)
    event_datetime = datetime.utcfromtimestamp(data['event_time'] / 1000000)
    data['event_time'] = event_datetime.isoformat()

    reference_date = datetime(1970, 1, 1)
    converted_date = reference_date + timedelta(days=data['start_date'])
    data['start_date'] = converted_date.isoformat()
    
    # print(data)

    payload = [{'event_time': data['event_time'],'event_type': data['event_type'],'product_id': data['product_id'],'category_id': data['category_id'],'category_code': data['category_code'],'brand': data['brand'],'price': data['price'],'revenue': data['revenue'],'user_id': data['user_id'],'user_session': data['user_session'],'first_Name': data['first_name'],'last_Name': data['last_name'],'Email': data['email'],'phone_Number': data['phone_number'],'Country': data['country'], 'City':data['city'],'Gender': data['gender'],'Age': data['age'],'campion_1': data['campion_1'],'campion_2': data['campion_2'],'campion_3': data['campion_3'],'campion_4': data['campion_4'],'campion_5': data['campion_5'],'start_date': data['start_date'],'used_discount': data['used_discount'] }]
    
    
    
    
    # payload = [{'transaction_id': row['transaction_id'], 'customer_id': row['customer_id'], 'Country': row['Country'],  'City': row['City'], 'combined_Event': row['combined_Event'], 'product_id': row['product_id'], 'Revenue': row['Revenue']}]  # Adjust column names as per your CSV
    response = requests.post(api_url, json=payload)

    if response.status_code == 200:
        print(f"Data sent successfully: {payload}")
    else:
        print(f"Error sending data: {response.status_code} - {response.text}")


bootstrap_servers = ['kafka:9092']
consumer = KafkaConsumer( bootstrap_servers=bootstrap_servers)
#consumer.topics()
topicName = 'Ecom.public.transaction'
# Initialize consumer variable
consumer = KafkaConsumer (topicName , auto_offset_reset='earliest',  bootstrap_servers = bootstrap_servers, group_id='StreamingTransaction')


for msg in consumer:

    data = json.loads(msg.value)
    # print(data)
    TransactionData =  data
    
    send_data_to_power_bi(TransactionData)


consumer.close()    



