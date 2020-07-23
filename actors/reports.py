"""
New and improved reports.py module. Able to be ran locally or remotely.

Able to be ran with 'python3 reports.py'.

Fill in mongo_user, mongo_pass, mongo_host, and mongo_port on like 20 for remote runs.

Note, when there are a lot of executions, some docker containers may crash, 
thus the need for remote running of this script.

Other note, the 'get_actors_executions_for_report()' function can return a 
minimal or very large summary report. Default is minimal. To receive full report,
change the return for the function to the noted return.
"""

import urllib.parse
from pymongo import MongoClient

# Get mongo uri information from either config or manual entry.
# Inputs are mongo user, pass, host, and port.
try:
    from common.config import conf
    mongo_user = urllib.parse.quote_plus(conf.store_mongo_user)
    mongo_pass = urllib.parse.quote_plus(conf.store_mongo_password)
    mongo_host = conf.store_mongo_host
    mongo_port = conf.store_mongo_port
    mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}"
    print(f"mongo uri: {mongo_uri}")
except Exception as e:
    ###### FILL THIS IN FOR REMOTE RUNNING
    mongo_user = ''
    mongo_pass = ''
    mongo_host = ''
    mongo_port = ''
    mongo_user = urllib.parse.quote_plus(mongo_user)
    mongo_password = urllib.parse.quote_plus(mongo_pass)
    if mongo_host and not mongo_user and not mongo_pass:
        mongo_uri = f"mongodb://{mongo_host}:{mongo_port}"
        print(f"mongo uri: {mongo_uri}")
    else:
        mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}"
        print(f"mongo uri: {mongo_uri}")

    if not mongo_user or not mongo_pass or not mongo_host:
        print("Please manually enter mongo user, password, and host.")
        raise

# Create some Mongo things.
client = MongoClient(mongo_uri)
abacoDB = client.abaco

def get_actors_executions_for_report():
    """
    Collect actor and execution data about an Abaco instance for reporting purposes.
        
    :return: 
    """
    # Initializing result
    print("Starting report creation!")
    result = {'summary': {'total_actors_all': 0,
                          'total_actors_all_with_executions': 0,
                          'total_executions_all': 0,
                          'total_execution_runtime_all': 0,
                          'total_execution_cpu_all': 0,
                          'total_execution_io_all': 0,
                          'total_actors_existing': 0,
                          'total_actors_existing_with_executions': 0,
                          'total_executions_existing': 0,
                          'total_execution_runtime_existing': 0,
                          'total_execution_cpu_existing': 0,
                          'total_execution_io_existing': 0,},
              'actors': []}
    actor_info_dict = {}
    actors_with_executions = set()
    count = 0
    # Pulling all database data at once because it's what tends to break
    # and it takes the longest when running. This allows quick access to 
    # data.
    print("Pulling all executions now. This should take less than 120s.")
    execution_big_list = list(abacoDB['3'].find({}))
    print("Pulling all actors now.")
    actor_big_list = list(abacoDB['5'].find({}))
    print("Pulling all metrics now.")
    metrics_big_list = list(abacoDB['10'].find({}))
    print(f"Done pulling, now going through all {len(execution_big_list)} executions.")
    
    # Going through all executions to determine if they're part of an 
    # existing or deleted actor. Adds results to correct summary stanza.
    for execution in execution_big_list:
        # Get some imporant execution metrics.
        actor_id = execution['actor_id']
        actor_cpu = execution['cpu']
        actor_io = execution['io']
        actor_runtime = execution['runtime']
        # Add actor_id to list of actors with executions.
        actors_with_executions.add(actor_id)
        # Some totals to always add from executions.
        result['summary']['total_executions_all'] += 1
        result['summary']['total_execution_runtime_all'] += actor_runtime
        result['summary']['total_execution_io_all'] += actor_io
        result['summary']['total_execution_cpu_all'] += actor_cpu

        for actor in actor_big_list:
            # Determine if actor still exists.
            if actor['db_id'] == actor_id:
                # Create actor entry if not already accounted for.
                if not actor_id in actor_info_dict:
                    actor_info_dict[actor_id] = {'actor_id': actor.get('id'),
                                             'dbid': actor_id,
                                             'owner': actor.get('owner'),
                                             'image': actor.get('image'),
                                             'total_executions': 0,
                                             'total_execution_cpu': 0,
                                             'total_execution_io': 0,
                                             'total_execution_runtime': 0}

                # Write actor information if actor does exist.
                actor_info_dict[actor_id]['total_executions'] += 1
                actor_info_dict[actor_id]['total_execution_runtime'] += actor_runtime
                actor_info_dict[actor_id]['total_execution_io'] += actor_io
                actor_info_dict[actor_id]['total_execution_cpu'] += actor_cpu
                # Write result information if actor does exist.
                result['summary']['total_executions_existing'] += 1
                result['summary']['total_execution_runtime_existing'] += actor_runtime
                result['summary']['total_execution_io_existing'] += actor_io
                result['summary']['total_execution_cpu_existing'] += actor_cpu
        # Fun print lines because this sometimes takes a bit.
        count += 1
        if count % 25000 == 0:
            print(f"Current execution count: {count}", end="\r", flush=True)
    print(f"Current execution count: {count}")

    # Write some summary keys based on existing data.
    result['summary']['total_actors_existing_with_executions'] = len(actor_info_dict)
    result['summary']['total_actors_all_with_executions'] = len(actors_with_executions)
    result['summary']['total_actors_all'] += metrics_big_list[0]['actor_total']
    result['summary']['total_actors_existing'] += len(actor_big_list)

    # Return either full result or just the summary. 
    # Full result contains lots of information about existing actors.
    #return result
    return result['summary']

if __name__ == "__main__":
    print(get_actors_executions_for_report())