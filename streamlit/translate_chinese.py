import streamlit as st
import boto3

your_region='us-east-1'
# Function to perform translation
def translate_text(text, source_language_code, target_language_code):
    client = boto3.client(service_name='translate', region_name=your_region, use_ssl=True)
    result = client.translate_text(Text=text, 
                                   SourceLanguageCode=source_language_code, 
                                   TargetLanguageCode=target_language_code)
    return result.get('TranslatedText')

# Streamlit app layout
st.title('Mandarin to English Translator')

# Text input
input_text = st.text_area("Enter text in Mandarin:")

# Translate button
if st.button('Translate'):
    if input_text:
        # Call the translate function
        translated_text = translate_text(input_text, 'zh', 'en')
        # Display the translated text
        st.write('Translated text:')
        st.write(translated_text)
    else:
        st.write('Please enter some text to translate.')
