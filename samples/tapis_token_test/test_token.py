from agavepy.actors import get_context, get_client
import requests
import time
import datetime
import os

try:
    TOTAL_RUNS = int(os.environ.get('runs', '5'))
except Exception as e:
    print(f"got exception getting TOTAL_RUNS; defaulting to 10. e:{e}")
    TOTAL_RUNS = 10

verbose = False
try:
    verbose = os.environ.get('verbose', False)
except:
    print(f"verbose option not set.")

try:
    SLEEP = int(os.environ.get('sleep', '1'))
except Exception as e:
    print(f"got exception getting SLEEP; defaulting to 1 second. e:{e}")
    SLEEP = 1

print(f"TOTAL_RUNS: {TOTAL_RUNS}; SLEEP: {SLEEP}\n")


def tapis_call(ag):
    try:
        s = ag.systems.list()
        print(f"Call to systems list successful.")
        if verbose:
            print(f"output from systems list: {s}; now: {datetime.datetime.now()}\n")
    except Exception as e:
        print(f"systems list error; now: {datetime.datetime.now()}; e: {e}")


def main():
    print(f"executing; now: {datetime.datetime.now()}")
#    print(f"env: {os.environ}")
    context = get_context()
    ag = get_client()
#    print(f"got client; now: {datetime.datetime.now()}")

    try:
        print(f"ag._token: {ag._token}")
    except Exception as e:
        print(f"Got exception trying to read ag._token: {e}")


    # try:
    #     print(f"token_info: {ag.token.token_info}")
    # except Exception as e:
    #     print(f"Got exception trying to read token_info: {e}")


    try:
        access_token = os.environ.get('_abaco_access_token')
        print(f"Got abaco access token: {access_token}")
    except Exception as e:
        print(f"Could not get access token from abaco; e: {e}")

    # try to use the agavepy client to call tapis --
    print("\n\n*********\n\n")
    for i in range(TOTAL_RUNS):
        tapis_call(ag)
        time.sleep(SLEEP)

    # try to call Tapis with the abaco token using requests --
    for i in range(TOTAL_RUNS):
        print("\n\n*********\n\n")
        headers = {"Authorization": f"Bearer {access_token}"}
        try:
            rsp = requests.get("https://api.tacc.utexas.edu/systems/v2", headers=headers)
            rsp.raise_for_status()
            print("got 200 with abaco token!")
        except Exception as e:
            print(f"Got exception trying to call tapis with the abaco token. e: {e}")

    print(f"exiting, now: {datetime.datetime.now()}")
    print(f"env: {os.environ}")

if __name__ == '__main__':
    main()
