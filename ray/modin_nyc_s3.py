import modin.pandas as pd
import awswrangler as wr
import ray
import modin.pandas as pd
import glob
import uuid
import os

@ray.remote
def cp_file (BUCKET_NAME, OBJECT_NAME, FILE_NAME):
    import boto3
    s3_c = boto3.client('s3')
    s3_c.download_file(BUCKET_NAME, OBJECT_NAME, FILE_NAME)

if __name__ == '__main__':
  ls3=wr.s3.list_objects('s3://nyc-tlc/trip data/*yellow*2022*')
  tf = str(uuid.uuid4()).split('-')[-1]
  os.system(f'mkdir {tf}')
  [cp_file.remote('nyc-tlc',x.replace('s3://nyc-tlc/',''),f"{tf}/{x.replace('s3://nyc-tlc/','').split('/')[-1]}") for x in ls3yc]
  df_a = pd.concat([pd.read_parquet(x) for x in glob.glob(f'{tf}/*')])
  print(df_a.head())
