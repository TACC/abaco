from agavepy.agave import Agave
from agavepy.actors import AbacoExecutor

import numpy as np


# define some function you want to run in the cloud:
def f1(a, b):
    return a + b

# OR
def f(std_dev, size):
    A = np.random.normal(0, std_dev, (size, size))
    B = np.random.normal(0, std_dev, (size, size))
    C = np.dot(A, B)
    r = np.linalg.eig(C)[0]
    return r

def g(std_dev, size):
    A = np.random.normal(0, std_dev, (size, size))
    B = np.random.normal(0, std_dev, (size, size))
    C = np.dot(A, B)
    return C[0]

# instantiate an agavepy client using your access token:
ag = Agave(api_server='https://dev.tenants.staging.tacc.cloud', token='<your_token_here>')

# instantiate an AbacoExecutor using the agavepy client. This takes a little while to return as it registers an actor
# and waits for it's workers to be ready. By default it starts one worker but you can have it start more by passing
# the "workers" parameter
ex = AbacoExecutor(ag, image='abacosamples/py3_sci_base_func', num_workers=4)

# Call the function, f1,  asynchronously with the parameters 10 and 20, and "manually" manage the response:
arsp = ex.submit(f1, 10, 20)

# check if the result is ready -- returns True/False immediately
arsp.done()

# block until the execution completes and then return a list of all results
arsp.result()
# Out: [30]

# alternatively, call f 1and block and execution completes and return a list of all results.
ex.blocking_call(f1, 50, 107)
# Out: [157]

# map the function over a list of data:
import time
start=time.time()
arsp = ex.map(f, [[100, 4096] for i in range(8)])
for a in arsp:
    try:
        print(a.result())
    except Exception:
        print("error for execution: {}".format(a.execution_id))
end=time.time()
print("total time: {}".format(end-start))

eids = [a.execution_id for a in arsp]
for e in eids:
    rsp = ag.actors.getExecution(actorId=ex.actor_id, executionId=e)
    print(rsp.runtime, rsp.messageReceivedTime, rsp.finalState['StartedAt'], rsp.finalState['FinishedAt'], rsp.workerId)


# get workers:
wks = ag.actors.listWorkers(actorId=ex.actor_id)
for w in wks: print(w.id, w.status, w.cid)


start=time.time()
g(100, 8192)
end=time.time()
print("total time: {}".format(end-start))


# delete the executor when done to clean up the actor and workers...
ex.delete()