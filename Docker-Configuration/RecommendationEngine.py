
import zipfile
import os
from pyspark.ml.recommendation import  ALSModel
from pyspark.sql import SparkSession

from kafka import KafkaConsumer
import pandas as pd
import json
from confluent_kafka import Producer
from kafka import KafkaProducer
import uuid

print("Oooops_0")
# Create a SparkSession
spark= SparkSession.builder.appName("test_pyspark").getOrCreate()
# Initialize Spark session

sc = SparkSession.builder.appName("Product_Recommendation") \
.config ("spark.sql.shuffle.partitions", "16") \
.config("spark.driver.maxResultSize","4g") \
.config ("spark.sql.execution.arrow.enabled", "true") \
.config("spark.driver.memory", "4g") \
.config("spark.executor.cores", "4") \
.getOrCreate()

sc.sparkContext.setLogLevel("ERROR")


# Specify the name of the zip file to extract
zip_file_name = "/app/als_model.zip"

# Specify the directory where you want to extract the model
extracted_directory = "/app/extracted_model_directory"

# Create the directory if it doesn't exist
if not os.path.exists(extracted_directory):
    os.makedirs(extracted_directory)

# Extract the zip file to the specified directory
with zipfile.ZipFile(zip_file_name, 'r') as zipf:
    zipf.extractall(extracted_directory)

als_model = ALSModel.load('/app/extracted_model_directory')


bootstrap_servers = ['kafka:9092']
consumer = KafkaConsumer( bootstrap_servers=bootstrap_servers)
#consumer.topics()
topicName = 'Ecom.public.transaction'
# Initialize consumer variable
consumer = KafkaConsumer (topicName , auto_offset_reset='earliest',  bootstrap_servers = bootstrap_servers, group_id='recommendationConnector')

# Initialize Kafka producer
producer_conf = {
    'bootstrap.servers': 'kafka:9092',  #  Kafka broker address
}

producer = Producer(producer_conf)

Recommendation_topic = "RecommendationEngine"


for msg in consumer:
    

    data = json.loads(msg.value)
    userID =  data['payload']['user_id']
    
    
# Recommend top 500 products for the users
    recommendations = sc.createDataFrame([(userID, 0)], ['user_id', 'product_id'])
    recommendations = als_model.recommendForUserSubset(recommendations, 3)
    recommendations_df = recommendations.toPandas()



    try:
    # Attempt to access the first row and convert it to JSON
        json_Recommendation = recommendations_df.iloc[0].to_json()
        
        print("Successfully converted first row to JSON")
    except IndexError as e:
        print("Error: Unable to access first row. IndexError occurred.")
        print(e)
    except Exception as e:
        print("An unexpected error occurred:")
        print(e)
    
    # Send the JSON data to Kafka topic
    producer.produce(topic=Recommendation_topic, value=json_Recommendation)

    




    

consumer.close()    
producer.flush()


