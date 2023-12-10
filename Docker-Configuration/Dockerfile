#!/bin/sh
#!/bin/bash

# Use an official Python runtime as a parent image
FROM python:3.9
# FROM gcr.io/distroless/python3
# FROM ubuntu:latest

# Set the working directory in the container
WORKDIR /app


# Install Java
RUN apt-get update && \
    apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*

# Install Python 3 and pip3
# RUN apt-get update && \
#     apt-get install -y python3 && \
#     apt-get install -y python3-pip && \
#     rm -rf /var/lib/apt/lists/*

# Copy the local requirements file to the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Copy the local script to the container at /app
ADD streamingdata.py /app/streamingdata.py

ADD als_model.zip /app/als_model.zip
ADD RecommendationEngine.py /app/RecommendationEngine.py
ADD UploadStreamingTransaction.py /app/UploadStreamingTransaction.py
ADD UploadStreamingRecommendation.py /app/UploadStreamingRecommendation.py




# Install any needed packages specified in requirements.txt
RUN pip install kafka-python
RUN pip install --upgrade pyarrow
RUN pip install avro-python3



# Copy the entrypoint script to the container at /app
ADD entrypoint.sh /app/entrypoint.sh

RUN ["chmod", "+x", "./entrypoint.sh"]
CMD ["sh","/app/entrypoint.sh"]


# Run streaming_script.py when the container launches
# CMD ["python", "RecommendationEngine.py"]
