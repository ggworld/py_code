#by: Geva
# Geva pict/movide headers and ditails 
# max at the time of download : 1161639 
# got till 166750 exception ConnectionError
import ray
import boto3
import requests 
import pickle
import itertools
from io import BytesIO
import logging
import json
import time
starting_time = time.time()
print(starting_time)
ray.init()
DATA_BUCKET = 'hamal-red-feed-excaliber-data'

# Create a logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter for the log messages
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# Set the formatter for the console handler
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)

# Log messages at different levels

@ray.remote
def upload_file(obj,object_key,destination_bucket=DATA_BUCKET):
    import awswrangler as wr
    json_data = BytesIO(json.dumps(obj).encode('utf-8'))
    wr.s3.upload(path=f's3://{destination_bucket}/files/{object_key}.json', local_file=json_data)
    


@ray.remote
def get_res(skip):
    import awswrangler as wr
    # print(skip)
    destination_bucket = DATA_BUCKET
    object_key = skip
    url = 'https://api.iron-swords.com/files?skip='+str(skip)
    
    try:
        response = requests.get(url,
        headers={
            'Authorization': 'Bearer <get token>'
        },)
        # print(response.status_code)
        # print(response.json())
        if response.status_code == 200:
            # Get the JSON data as BytesIO object
            # json_data = BytesIO(json.dumps(response.json()).encode('utf-8'))
            res_up = []
            for obj in response.json()['files']:
                res_up.append(upload_file.remote(obj,object_key))
                # json_data = BytesIO(json.dumps(obj).encode('utf-8'))
                # wr.s3.upload(path=f's3://{destination_bucket}/files/{object_key}.json', local_file=json_data)
                object_key+=1
            tmp_all_finish = ray.get(res_up)


        # json_string = '\n'.join([json.dumps(obj) for obj in response.json()['files']])

        # json_data = BytesIO(json_string.encode('utf-8'))

        # Load the JSON data into a Python dictionary
        # data_dict = json.load(json_data)

        # Convert the Python dictionary back to a JSON string (optional)
        # json_string = json.dumps(data_dict)

        # Upload the JSON data to S3 using AWS Data Wrangler
        # wr.s3.upload(path=f's3://{destination_bucket}/files/{object_key}.json', local_file=json_data)

        # print(f'JSON data uploaded to s3://{destination_bucket}/files/{object_key}')
        else:
            print('Failed to download the JSON data from the URL')
    except ConnectionError:
        print(f"ConnectionError in {object_key}")
        with open('connection_err.txt','a') as err_f:
            err_f.write(str(object_key)+'\n')
            object_key+=1


    # copy_to_s3.remote(skip,response.json())

    return(skip)

if __name__=="__main__":
    # ray.init()
    # refs = [get_res.remote (i*50) for i in range(3)]
    # # print(l)

    refs = []
    for i in range(3333,23232):
        print(i)
        refs.append(get_res.remote(i*50))

    parallel_returns = ray.get(refs)

    print(f"done in {time.time()-starting_time}")
    # list_files = ray.get(l)
    #Save the list to a pickle file
