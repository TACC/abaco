import os
import time

from agavepy.agave import Agave


class AbacoTimer(object):
    """ Utility class for collecting timing data against Abaco.
    """

    def __init__(self,
                 token,
                 api_server='https://dev.tenants.staging.tacc.cloud',
                 image='abacosamples/test'):
        self.token = token
        self.api_server = api_server
        self.image = image
        self.client = self.get_client()
        self.actor_id = self.register_actor()
        self.executions = []
        self.in_process_executions = []
        self.workers = self.get_workers()

    def get_client(self):
        return Agave(token=self.token, api_server=self.api_server)

    def register_actor(self):
        try:
            rsp = self.client.actors.add(body={'image': self.image})
        except Exception as e:
            print("Could not register the actor: {}".format(e))
            raise e
        return rsp.id

    def get_workers(self):
        return self.client.actors.listWorkers(actorId=self.actor_id)

    def add_workers(self, num):
        try:
            self.client.actors.addWorker(actorId=self.actor_id, body={'num': num})
        except:
            pass


    def send_message(self, message='test'):
        try:
            rsp = self.client.actors.sendMessage(actorId=self.actor_id, body={'message': message})
        except Exception as e:
            print("Could not register the actor: {}".format(e))
            raise e
        self.executions.append(rsp.executionId)
        self.in_process_executions.append(rsp.executionId)

    def get_messages(self):
        return self.client.actors.getMessages(actorId=self.actor_id).messages

    def time_number_of_messages(self, num):
        """Returns the time to execute `num` number of message."""
        start_time = time.time()
        for i in range(num):
            self.send_message()
        while True:
            if self.all_executions_complete():
                break
            else:
                time.sleep(0.1)
        end_time = time.time()
        return end_time-start_time


    def all_executions_complete(self):
        for e in self.in_process_executions:
            rsp = self.client.actors.getExecution(actorId=self.actor_id, executionId=e)
            if not rsp.status == 'COMPLETE':
                return False
            else:
                try:
                    self.in_process_executions.remove(e)
                except:
                    pass
        return True


def main():
    print("Top of main.")
    NUM_MESSAGES = 100
    NUM_WORKERS = 20
    token = os.environ.get('ABACO_TOKEN')
    print("Using token: {}".format(token))
    t = AbacoTimer(token=token)
    print("AbacoTimer created. actor id: {}".format(t.actor_id))
    start_time = time.time()
    t.add_workers(NUM_WORKERS)
    print("workers added.")
    send_messages_start_time = time.time()
    for i in range(NUM_MESSAGES):
        t.send_message()
    send_messages_end_time = time.time()
    print("Messages sent")
    while True:
        if t.all_executions_complete():
            break
        else:
            print("Number of pending executions: {}".format(len(t.in_process_executions)))
            print("Number of messages in the queue: {}".format(t.get_messages()))
            time.sleep(1)
    end_time = time.time()
    print("\n\n")
    print("Final times:")
    print("===========")
    print("Total: {}".format(end_time - start_time))
    print("Executions: {}".format(end_time - send_messages_end_time))
    print("Sending messages: {}".format(send_messages_end_time - send_messages_start_time))
    print("Adding workers: {}".format(send_messages_start_time - start_time))
    print("Raw times: {}, {}, {}, {}".format(start_time, send_messages_start_time, send_messages_end_time, end_time))

if __name__ == '__main__':
    main()