from agavepy.agave import Agave
from agavepy.actors import AbacoExecutor

# define some function you want to run in the cloud:
def f(a, b):
    return a + b

# instantiate an agavepy client using your access token:
ag = Agave(api_server='https://dev.tenants.staging.tacc.cloud', token='<your_token_here>')

# instantiate an AbacoExecutor using the agavepy client. This takes a little while to return as it registers an actor
# and waits for it's workers to be ready. By default it starts one worker but you can have it start more by passing
# the "workers" parameter
ex = AbacoExecutor(ag)

# Call the function, f,  asynchronously with the parameters 10 and 20, and "manually" manage the response:
arsp = ex.submit(f, 10, 20)

# check if the result is ready -- returns True/False immediately
arsp.done()

# block until the execution completes and then return a list of all results
arsp.result()
# Out: [30]

# alternatively, call f and block and execution completes and return a list of all results.
ex.blocking_call(f, 50, 107)
# Out: [157]

# delete the executor when done to clean up the actor and workers...
ex.delete()