# Import Libraries
import requests
import pandas as pd
import time

from kafka import KafkaConsumer
import pandas as pd
import json
import uuid

# Define your Power BI API endpoint and API key
api_url = 'https://api.powerbi.com/beta/635448dd-5608-444b-b9de-b06a0f8fb81f/datasets/b985f139-f1b4-48ea-bf00-2dc86e775c87/rows?redirectedFromSignup=1&experience=power-bi&key=bylG9%2FVNA%2BhRRSkIG9kDtQth6CphlFZKmKVYLrPe82Qnnhcy3cu%2BjN7SKEd5OG2NQuGhKuGLDTGGX9YmyFuMbg%3D%3D'
# Read data from CSV (assuming your CSV has columns named 'Column1' and 'Column2')


# Function to send data to Power BI
def send_data_to_power_bi(recommendations_data):
    i = 1
    for recommendation in recommendations_data['recommendations']:
        print(recommendations_data)
        payload = [{"product_id" :recommendation['product_id'],"user_id" :recommendations_data['user_id'],"Seq" :i,"ratings" :recommendation['rating']}]
        response = requests.post(api_url, json=payload)
        i = i+1

        if response.status_code == 200:
            print(f"Recommendation for product {recommendation['product_id']} sent successfully.")
        else:
            print(f"Failed to send recommendation for product {recommendation['product_id']}. Status code: {response.status_code}")


bootstrap_servers = ['kafka:9092']
consumer = KafkaConsumer( bootstrap_servers=bootstrap_servers)
#consumer.topics()
topicName = 'RecommendationEngine'
# Initialize consumer variable
consumer = KafkaConsumer (topicName , auto_offset_reset='earliest',  bootstrap_servers = bootstrap_servers, group_id='StreamingRecommendation')



for msg in consumer:
    

    data = json.loads(msg.value)
    # print(data)
    ProductData =  data
    
    send_data_to_power_bi(ProductData)

