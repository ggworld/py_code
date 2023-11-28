import streamlit as st
import pytesseract
from PIL import Image
import io

def extract_text_from_image(image_body):
    try:
        # Open the image file
        #img = Image.open(image_path)

        # Use pytesseract to do OCR on the image
        bytes_data = image_body.getvalue()

    # Convert the bytes data to a PIL Image object
        image = Image.open(io.BytesIO(bytes_data))

        text = pytesseract.image_to_string(image)
    # Now you can use 'image' in your application

        print('got',text)
        return text
    except Exception as e:
        print ('exception')
        return str(e)
picture = st.camera_input("Take a picture")

if picture:
    st.image(picture)
    extracted_text = extract_text_from_image(picture)
    st.title(extracted_text)
