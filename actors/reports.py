from config import Config
from models import Actor
from stores import actors_store, executions_store, workers_store



def get_actors_executions_for_report():
    """
    Collect actor and execution data about an Abaco instance for reporting purposes.
     
    :return: 
    """
    result = {'summary': {'total_actors_all': 0,
                          'total_executions_all': 0,
                          'total_execution_runtime_all': 0,
                          'total_execution_cpu_all': 0,
                          'total_execution_io_all': 0,
                          'total_actors_existing': 0,
                          'total_executions_existing': 0,
                          'total_execution_runtime_existing': 0,
                          'total_execution_cpu_existing': 0,
                          'total_execution_io_existing': 0,
                          },
              'actors': []
              }

    current = 0
    for actor in executions_store.items(proj_inp= None):
        dbid = actor['_id']
        del actor['_id']
        # determine if actor still exists:
        actor = None
        # there are "history" records which batch large numbers of executions for
        # very active actors. the key for these records is of the form:
        #  <actor_dbid>_HIST_<hist_id>
        if '_HIST_' in dbid:
            dbid = dbid.split('_HIST_')[0]

        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            pass
        # iterate over executions for this actor:
        actor_exs = 0
        actor_runtime = 0
        actor_io = 0
        actor_cpu = 0
        for _, execution in actor.items():
            current += 1
            print(f"processing execution {current}")
            actor_exs += 1
            actor_runtime += execution.get('runtime', 0)
            actor_io += execution.get('io', 0)
            actor_cpu += execution.get('cpu', 0)
        # always add these to the totals:
        result['summary']['total_actors_all'] += 1
        result['summary']['total_executions_all'] += actor_exs
        result['summary']['total_execution_runtime_all'] += actor_runtime
        result['summary']['total_execution_io_all'] += actor_io
        result['summary']['total_execution_cpu_all'] += actor_cpu

        if actor:
            result['summary']['total_actors_existing'] += 1
            result['summary']['total_executions_existing'] += actor_exs
            result['summary']['total_execution_runtime_existing'] += actor_runtime
            result['summary']['total_execution_io_existing'] += actor_io
            result['summary']['total_execution_cpu_existing'] += actor_cpu
            actor_stats = {'actor_id': actor.get('id'),
                           'dbid': dbid,
                           'owner': actor.get('owner'),
                           'image': actor.get('image'),
                           'total_executions': actor_exs,
                           'total_execution_cpu': actor_cpu,
                           'total_execution_io': actor_io,
                           'total_execution_runtime': actor_runtime,
                           }
            result['actors'].append(actor_stats)
    return result