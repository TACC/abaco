"""
The purpose of this module is to test for a leak in rabbitmq connections and queues that results from use
of the anonymous channel.

What we see in typical use is a growing number of RabbitMQ connections and queues through the Abaco lifecycle. In this
module, we reproduce the behavior and then experiment with potential fixes.

To start the test, first launch two containers using the abaco/testsuite:dev image in two separate terminals.

# Terminal 1
$ docker run -it --name server1 --rm --entrypoint=bash abaco/testsuite:dev
bash-4.3# python3 /tests/channel_tests.py 1

# Terminal 2
$ docker run -it --name server2 --rm --entrypoint=bash abaco/testsuite:dev
bash-4.3# python3 /tests/channel_tests.py 2

Then, observer the behavior on the RabbitMQ admin console
(http://localhost:15672/#/queues and http://localhost:15672/#/connections)
"""

import argparse
import os
import sys
import threading
import time

sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')

from actors.channels import SpawnerWorkerChannel


def server1():
    print("server1 started.")
    ch = SpawnerWorkerChannel(worker_id='foo')
    while True:
        r = ch.get()
        print("server1 got: {}".format(r['value']))
        r['reply_to'].put('ok')
        # ----- HERE is the experimental work ----------
        # ideally, we would delete the anonymous channel from this thread but there is a race condition:
        # we need server2 to receive the `ok` message BEFORE we delete the channel.
        # this code can be commented out to see the leak behavior:
        time.sleep(1.5)
        r['reply_to'].delete()
        # ----- experimental block ends ----------------


def server2():
    print("server2 started.")
    while True:
        ch = SpawnerWorkerChannel(worker_id='foo')
        rsp = ch.put_sync('test')
        print("server2 got {}".format(rsp))
        time.sleep(1)
        ch.close()
        print("server2 closed its foo channel.")

if __name__ == '__main__':
    # print("Starting server1 in separate thread.")
    # t = threading.Thread(target=server1)
    # t.start()
    # print("Waiting for server1.")
    # time.sleep(1)
    # print("Starting server2.")
    # server2()
    parser = argparse.ArgumentParser(description='Two server test for channelpy.')
    parser.add_argument('server', type=int,
                        help='The server to start.')
    args = parser.parse_args()
    if args.server == 1:
        print("Starting server1...")
        server1()
    else:
        print("Starting server2...")
        server2()
