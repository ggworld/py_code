import awswrangler as wr
import pandas as pd 
cloudwatch_query = """stats count(*) as value by bin(1m) as date | sort date  """
log_group = f"/aws/vendedlogs/states/thairelease_aiola_transcript_inspection"
# from_timestamp = '2022-08-01'
# to_timestamp = '2023-02-25'

df_dates = wr.cloudwatch.read_logs(
    log_group_names=[log_group],
    query=cloudwatch_query,
    limit=10000,  # the default is 1000, which can be not enough in some use cases
)



cloudwatch_query = """fields @timestamp, @message"""
all_parts = []
for my_date in get_dates(df_dates):
    all_parts.append(wr.cloudwatch.read_logs(
    log_group_names=[log_group],
    query=cloudwatch_query,
    start_time=my_date[0],
    end_time=my_date[1],
    limit=10000,  ))


all_data = pd.concat(all_parts)
all_data
