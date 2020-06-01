from models import Actor
from stores import actors_store, executions_store, workers_store, abaco_metrics_store

def get_actors_executions_for_report():
    """
    Collect actor and execution data about an Abaco instance for reporting purposes.
        
    :return: 
    """
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
    actor_stats = {}
    actor_does_not_exist = []
    for execution in executions_store.items():
        actor_id = execution['actor_id']
        actor_cpu = execution['cpu']
        actor_io = execution['io']
        actor_runtime = execution['runtime']
        if actor_id in actor_does_not_exist:
            pass
        else:
            try:
                # checks if actor existance has already been tested
                if not actor_id in actor_stats:
                    result['summary']['total_actors_all_with_executions'] += 1
                    # determine if actor still exists:
                    actor = Actor.from_db(actors_store[actor_id])
                    # creates dict if actor does exist
                    actor_stats[actor_id] = {'actor_id': actor.get('id'),
                                             'dbid': actor_id,
                                             'owner': actor.get('owner'),
                                             'image': actor.get('image'),
                                             'total_executions': 0,
                                             'total_execution_cpu': 0,
                                             'total_execution_io': 0,
                                             'total_execution_runtime': 0}
                    result['summary']['total_actors_existing_with_executions'] += 1

                # write actor information if actor does exist
                actor_stats[actor_id]['total_executions'] += 1
                actor_stats[actor_id]['total_execution_runtime'] += actor_runtime
                actor_stats[actor_id]['total_execution_io'] += actor_io
                actor_stats[actor_id]['total_execution_cpu'] += actor_cpu
                # write result information if actor does exist
                result['summary']['total_executions_existing'] += 1
                result['summary']['total_execution_runtime_existing'] += actor_runtime
                result['summary']['total_execution_io_existing'] += actor_io
                result['summary']['total_execution_cpu_existing'] += actor_cpu
            except KeyError:
                actor_does_not_exist.append(actor_id)
        # always add these to the totals:
        result['summary']['total_executions_all'] += 1
        result['summary']['total_execution_runtime_all'] += actor_runtime
        result['summary']['total_execution_io_all'] += actor_io
        result['summary']['total_execution_cpu_all'] += actor_cpu
        result['actors'] = list(actor_stats.values())

    result['summary']['total_actors_all'] += abaco_metrics_store['stats', 'actor_total']
    result['summary']['total_actors_existing'] += len(actors_store)
    return result
    
if __name__ == "__main__":
    print(get_actors_executions_for_report())