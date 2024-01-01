import streamlit as st
import boto3
import os
from PIL import Image
import io
from pyzbar.pyzbar import decode
import uuid 
import cv2
import numpy as np
import io


# Streamlit page configuration
st.set_page_config(page_title="Barcode Scanner", layout="wide")

# AWS credentials and settings
#aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
#aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region_name = 'us-east-1'
#put bucket name
s3_bucket_name = '****'

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=aws_region_name)
rekognition_client = boto3.client('rekognition',  region_name=aws_region_name)

# Streamlit interface to capture an image
st.title("Barcode Scanner")
captured_image = st.camera_input("Take a picture")

def upload_to_s3(image_bytes, bucket, file_name):
    s3_client.put_object(Body=image_bytes, Bucket=bucket, Key=file_name)

def process_image_with_rekognition(bucket, file_name):
    response = rekognition_client.detect_text(Image={'S3Object': {'Bucket': bucket, 'Name': file_name}})
    # This part can be modified based on how you want to handle the Rekognition response
    return response

def decode_barcode(image_bytes):
    image = Image.open(io.BytesIO(image_bytes))
    barcodes = decode(image)
    return barcodes

if captured_image is not None:
    # Generate a unique file name and upload to S3
    file_name = f"barcode-{uuid.uuid4()}.jpg"
    upload_to_s3(captured_image.getvalue(), s3_bucket_name, file_name)

    # Process the image with Rekognition (this step may be modified based on your needs)
    rekognition_response = process_image_with_rekognition(s3_bucket_name, file_name)

    # Download the image from S3 for local decoding (if necessary)
    image_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=file_name)
    image_data = image_obj['Body'].read()

    # Decode the barcode
    barcodes = decode_barcode(image_data)
    if barcodes:
        for barcode in barcodes:
            st.success(f"Barcode Detected: {barcode.data.decode('utf-8')}, Type: {barcode.type}")
    else:
        st.error("No barcode detected. Please try capturing the image again.")
    
    image_np_array = np.frombuffer(image_data, np.uint8)

    # Decode the image
    image = cv2.imdecode(image_np_array, cv2.IMREAD_COLOR)

    bd = cv2.barcode.BarcodeDetector()
    #retval, decoded_info, decoded_type, points = bd.detectAndDecode(cv_image)
    retval ,*otherinfo = bd.detectAndDecode(image)
    if retval:
        st.success(otherinfo[0])
