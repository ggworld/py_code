import pandas as pd
import datetime

# Create sample DataFrame
df = pd.DataFrame({'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04','2022-01-05', '2022-01-06'],
                   'value': [75, 25, 105, 60,99,100]})
def get_dates(df,target_sum=100):
  df['date'] = pd.to_datetime(df['date'])
  df['value'] = df.value.astype(int)
  # Specify the target sum value
  

  # Create a new column with the cumulative sum of the 'value' column
  df['cumulative_sum'],df['next'] = df['value'].astype(int).cumsum(),df['value'].shift(-1)

  # Create a list to store the date ranges of the groups
  date_ranges = []

  # Initialize a variable to store the start date of the current group
  start_date = None
  sum_till_now = 0
  # Iterate through the rows of the DataFrame
  for i, row in df.iterrows():
      if row['cumulative_sum'] - sum_till_now >= target_sum:
          if start_date:
              date_ranges.append((start_date, row['date']+datetime.timedelta(milliseconds=1)))
          else:
              date_ranges.append((row['date'], row['date']+datetime.timedelta(milliseconds=1)))
          sum_till_now = row['cumulative_sum']
          start_date = None
      elif row['cumulative_sum'] + row['next'] - sum_till_now > target_sum:
        if start_date:
            date_ranges.append((start_date, row['date']+datetime.timedelta(milliseconds=1)))
        else:
            date_ranges.append((row['date'], row['date']+datetime.timedelta(milliseconds=1)))
        sum_till_now = row['cumulative_sum']
        start_date = None
      else:
        if not start_date:
          start_date = row['date']
  # If the last group does not reach the target sum, add the last date as the end date
  if start_date:
      date_ranges.append((start_date, df['date'].iloc[-1]+datetime.timedelta(milliseconds=1)))
  return date_ranges

# Print the date ranges of the groups
print(get_dates(df))
