# Import Libraries
import requests
import pandas as pd
import time

from kafka import KafkaConsumer
import pandas as pd
import json
import uuid

# Define your Power BI API endpoint and API key
api_url = 'https://api.powerbi.com/beta/635448dd-5608-444b-b9de-b06a0f8fb81f/datasets/e5f5196d-b513-4f02-95b3-a6141479c492/rows?experience=power-bi&key=JD9ElvlejbIf6Ty%2BDN1YRLBwB2zqrhujF2QBnieSJTwihnOV9j67vONhbCOpTzjwfoOltMvXfQMZLZe7DNNdkw%3D%3D'
# Read data from CSV (assuming your CSV has columns named 'Column1' and 'Column2')
data = pd.read_csv('Product_Info.csv')

# Function to send data to Power BI
def send_data_to_power_bi(row):
    payload = [{'productID': row['product_id'], 'userID': row['user_id']}]  # Adjust column names as per your CSV
    response = requests.post(api_url, json=payload)

    # if response.status_code == 200:
    #     print(f"Data sent successfully: {payload}")
    # else:
    #     print(f"Error sending data: {response.status_code} - {response.text}")


bootstrap_servers = ['kafka:9092']
consumer = KafkaConsumer( bootstrap_servers=bootstrap_servers)
#consumer.topics()
topicName = 'RecommendationEngine'
# Initialize consumer variable
consumer = KafkaConsumer (topicName , auto_offset_reset='earliest',  bootstrap_servers = bootstrap_servers, group_id=uuid.uuid4().hex)



for msg in consumer:
    

    data = json.loads(msg.value)
    ProductData =  data['payload']
    
    send_data_to_power_bi(ProductData)

