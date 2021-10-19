from agavepy.actors import get_context, get_client
import time
import datetime
import os

try:
    TOTAL_RUNS = int(os.environ.get('runs', '10'))
except Exception as e:
    print(f"got exception getting TOTAL_RUNS; defaulting to 10. e:{e}")
    TOTAL_RUNS = 10

try:
    SLEEP = int(os.environ.get('sleep', '1'))
except Exception as e:
    print(f"got exception getting SLEEP; defaulting to 1 second. e:{e}")
    SLEEP = 1

print(f"TOTAL_RUNS: {TOTAL_RUNS}; SLEEP: {SLEEP}\n")


def tapis_call(ag):
    try:
        s = ag.systems.list()
        print(f"output from systems list: {s}; now: {datetime.datetime.now()}\n")
    except Exception as e:
        print(f"systems list error; now: {datetime.datetime.now()}; e: {e}")


def main():
    print(f"executing; now: {datetime.datetime.now()}")
    context = get_context()
    ag = get_client()
    print(f"got client; now: {datetime.datetime.now()}")
    for i in range(TOTAL_RUNS):
        tapis_call(ag)
        time.sleep(SLEEP)
    print(f"exiting, now: {datetime.datetime.now()}")
    print(f"environment: {os.environ}")

if __name__ == '__main__':
    main()