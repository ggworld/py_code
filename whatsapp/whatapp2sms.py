I #Using twillo api 
from flask import Flask, request
from twilio.rest import Client

app = Flask(__name__)


import os
# and set the environment variables. See http://twil.io/secure
account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']
client = Client(account_sid, auth_token)
class argsi:
    to= "<dest number>"

@app.route('/incoming-message', methods=['POST'])
def handle_incoming_message():
    body = request.form['Body']
    sender = request.form['From']
    to = 'your_destination_phone_number'
    #client.messages.create(
    #    body=body,
    #    from_=sender,
    #    to=to
    #)
    print(request.form['Body'])
    message = client.messages \
                .create(
                     body=body,
                     from_='<srs number >',
                     to=f'+{argsi.to}'
                 )
    return ''

~                   
