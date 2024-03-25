import ray
import time
import random
import uuid
from pymongo import MongoClient


# Initialize Ray
ray.init()

@ray.remote
def hello_task(counter,users,containers):
    # client = MongoClient("mongodb+srv://******")
  

# Step 2: Select the database
    db = client['aOt1']

# Step 3: Select the collection
    collection = db['prep_insert']
    my_time = random.randint(1709646107807,1709646107807+1395266193)
    flowExecutionId = str(uuid.uuid4()).split('-')[-1]
    tmep_rec = [{'flowExecutionId': flowExecutionId,
        'id': str(uuid.uuid4()).split('-')[-1],
        'containerId': random.choice(containers),
        'containerIdEventDefIdCreationDateClientTime': 'E7qFEBOF0V0Wbgvt-B4MW:Mv9uMH9925ExlZITzY9Rb:1709646107807',
        'creationDateClientTime': my_time,
        'creationDateServerTime': my_time,
        'eventDefId': 'Mv9uMH9925ExlZITzY9Rb',
        'generatedSource': random.choice(['voice','voice','voice','ui']),
        'generationMethod': 'userAction',
        'name': 'Temperature',
        'reportedValue': str(random.randint(-20,20)),
        'reportedValueType': 'Number',
        'type': 'data',
        'userId': random.choice(users)} for x in range(150)]
    start_time = time.time()  # Capture start time
    # Simulated task work by sleeping for a random amount of time
    # time.sleep(counter % 3)  # Varying the sleep time for demonstration
    insert_result = collection.insert_many(tmep_rec)
    elapsed_time = time.time() - start_time  # Calculate elapsed time
    return tmep_rec, elapsed_time

# Launching 10 parallel tasks
users = [str(uuid.uuid4()) for x in range(20)]
containers = [str(uuid.uuid4()) for x in range(1500)]
#this is a bug as it runs too many workers 
task_ids = [hello_task.remote(counter,users,containers) for counter in range(1, 247000)]

# Waiting for all tasks to complete and retrieving their results
results = ray.get(task_ids)

# Printing the results and their execution times

for result, elapsed_time in results:
    print(f"{result} ,{elapsed_time:.2f}")
