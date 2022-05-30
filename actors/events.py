"""
Create events. 
Check for subscriptions (actor links) and publish events to the Events Exchange.

new events:

# create a new event object:
event = ActorEvent(tenant_id, actor_id, event_type, data)

# handles all logic associated with publishing the event, including checking
event.publish()

"""
import json
import os
import rabbitpy
import requests
import time

from codes import SUBMITTED
from channels import ActorMsgChannel, EventsChannel
from models import Execution, site
from stores import actors_store
from tapisservice.logs import get_logger


# Give permissions to Docker copied folders and files.
# Have to do this as we are running as Tapis user, not root.
# This script requires no permissions.
os.system(f'sudo /home/tapis/actors/folder_permissions.sh /home/tapis/runtime_files')
os.system(f'sudo /home/tapis/actors/folder_permissions.sh /var/run/docker.sock')

logger = get_logger(__name__)

def process_event_msg(msg):
    """
    Process an event msg on an event queue
    :param msg: 
    :return: 
    """
    logger.debug(f"top of process_event_msg; raw msg: {msg}")
    try:
        tenant_id = msg['tenant_id']
    except Exception as e:
        logger.error(f"Missing tenant_id in event msg; exception: {e}; msg: {msg}")
        raise e
    link = msg.get('_abaco_link')
    webhook = msg.get('_abaco_webhook')
    logger.debug("processing event data; "
                 "tenant_id: {}; link: {}; webhook: {}".format(tenant_id, link, webhook))
    # additional metadata about the execution
    d = {}
    d['_abaco_Content_Type'] = 'application/json'
    d['_abaco_username'] = 'Abaco Event'
    if link:
        process_link(link, msg, d)
    if webhook:
        process_webhook(webhook, msg, d)
    if not link and not webhook:
        logger.error(f"No link or webhook. Ignoring event. msg: {msg}")

def process_link(link, msg, d):
    """
    Process an event with a link.
    :return: 
    """
    # ensure that the linked actor still exists; the link attribute is *always* the dbid of the linked
    # actor
    logger.debug("top of process_link")
    try:
        actors_store[site()][link]
    except KeyError as e:
        logger.error(f"Processing event message for actor {link} that does not exist. Quiting")
        raise e

    # create an execution for the linked actor with message
    exc = Execution.add_execution(link, {'cpu': 0,
                                         'io': 0,
                                         'runtime': 0,
                                         'status': SUBMITTED,
                                         'executor': 'Abaco Event'})
    logger.info(f"Events processor agent added execution {exc} for actor {link}")
    d['_abaco_execution_id'] = exc
    logger.debug(f"sending message to actor. Final message {msg} and message dictionary: {d}")
    ch = ActorMsgChannel(actor_id=link)
    ch.put_msg(message=msg, d=d)
    ch.close()
    logger.info("link processed.")

def process_webhook(webhook, msg, d):
    logger.debug("top of process_webhook")
    msg.update(d)
    try:
        rsp = requests.post(webhook, json=msg)
        rsp.raise_for_status()
        logger.debug("webhook processed")
    except Exception as e:
        logger.error("Events got exception posting to webhook: "
                     "{}; exception: {}; event: {}".format(webhook, e, msg))


def run(ch):
    """
    Primary loop for events processor agent.
    :param ch: 
    :return: 
    """
    while True:
        logger.info("top of events processor while loop")
        msg, msg_obj = ch.get_one()
        try:
            process_event_msg(msg)
        except Exception as e:
            msg = f"Events processor get an exception trying to process a message. exception: {e}; msg: {msg}"
            logger.error(msg)
        # at this point, all messages are acked, even when there is an error processing
        msg_obj.ack()


def main():
    """
    Entrypoint for events processor agent.
    :return: 
    """
    # operator should pass the name of the events channel that this events agent should subscribe to.
    #
    ch_name = os.environ.get('events_ch_name')

    idx = 0
    while idx < 3:
        try:
            if ch_name:
                ch = EventsChannel(name=ch_name)
            else:
                ch = EventsChannel()
            logger.info("events processor made connection to rabbit, entering main loop")
            logger.info(f"events processor using abaco_host_path={os.environ.get('abaco_host_path')}")
            run(ch)
        except (rabbitpy.exceptions.ConnectionException, RuntimeError):
            # rabbit seems to take a few seconds to come up
            time.sleep(5)
            idx += 1
    logger.critical("events agent could not connect to rabbitMQ. Shutting down!")

if __name__ == '__main__':
    # This is the entry point for the events processor agent container.
    logger.info("Inital log for events processor agent.")

    # call the main() function:
    main()
