import streamlit as st
from PIL import Image
import io
import boto3
import re
import io




# Initialize a boto3 client with Amazon Textract
client = boto3.client('textract',
                      region_name='us-east-1')  # replace with your region


def extract_number(text):
    # Regex pattern for a number like 7946 6697 8391
    pattern = r'\b\d{4} \d{4} \d{4}\b'

    matches = re.findall(pattern, text)
    return matches

def extract_package_code(text):
    # Example regex pattern for a package code (adjust according to your needs)
    pattern = r'\b[A-Z0-9]{10}\b'  # Adjust this pattern to match your package code format
    #AliExpress package pattern 
    pattern = r'\b[A-Z]{2}\d{14}\b'  # Two letters followed by 14 digits
    matches = re.findall(pattern, text)
    return matches

# ... [rest of your code to extract text using Textract] ...

# Use the function

# Now 'package_codes' contains a list of all found package codes

def extract_text_from_image_aws(image_body):
    bytes_data = image_body.getvalue()

    #image = Image.open(io.BytesIO(bytes_data))
    imageBytes = bytearray(bytes_data)
    # Convert the bytes data to a PIL Image object

    # Assuming bytes_data is your original byte array containing the image
    bytes_data = imageBytes

    # Convert the byte array to an image object
    image_bytes_io = io.BytesIO(bytes_data)
    image = Image.open(image_bytes_io)

    # to Rotate the image 270 degrees counter-clockwise
    rotated_image = image.rotate(270)

    # Save the rotated image back to a BytesIO object
    output_bytes_io = io.BytesIO()
    rotated_image.save(output_bytes_io, format='JPEG')  # Use the appropriate format for your image

    # Convert the BytesIO object back to a byte array
    rotated_image_bytes = output_bytes_io.getvalue()
    imageBytes = bytearray(rotated_image_bytes)
    st.image(output_bytes_io)


    # Call Amazon Textract
    response = client.detect_document_text(Document={'Bytes': imageBytes})

    # Print detected text
    my_txt=""
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            print(item['Text'])
            my_txt += ' '+item['Text']
            st.text(item['Text'])
    return my_txt

# Example usage

if __name__ == "__main__":
    picture = st.camera_input("Take a picture")

    if picture:
        extracted_text = extract_text_from_image_aws(picture)
        package_codes = extract_package_code(extracted_text)
        if len(package_codes)>0:
            st.title('Pakage is: ' + package_codes[0])
        #check for fedex tracking number if not found 
        else:
            tracking_number=extract_number(extracted_text)
            if len(tracking_number)>0:
                st.title('Track # is: ' +tracking_number[0])
