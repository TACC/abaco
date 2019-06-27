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
from agaveflask.auth import get_api_server

from codes import SUBMITTED
from channels import ActorMsgChannel, EventsChannel
from models import Execution
from stores import actors_store


from agaveflask.logs import get_logger
logger = get_logger(__name__)

def process_event_msg(msg):
    """
    Process an event msg on an event queue
    :param msg: 
    :return: 
    """
    logger.debug("top of process_event_msg; raw msg: {}".format(msg))
    try:
        tenant_id = msg['tenant_id']
    except Exception as e:
        logger.error("Missing tenant_id in event msg; exception: {}; msg: {}".format(e, msg))
        raise e
    link = msg.get('_abaco_link')
    webhook = msg.get('_abaco_webhook')
    logger.debug("processing event data; "
                 "tenant_id: {}; link: {}; webhook: {}".format(tenant_id, link, webhook))
    # additional metadata about the execution
    d = {}
    d['_abaco_Content_Type'] = 'application/json'
    d['_abaco_username'] = 'Abaco Event'
    d['_abaco_api_server'] = get_api_server(tenant_id)
    if link:
        process_link(link, msg, d)
    if webhook:
        process_webhook(webhook, msg, d)
    if not link and not webhook:
        logger.error("No link or webhook. Ignoring event. msg: {}".format(msg))

def process_link(link, msg, d):
    """
    Process an event with a link.
    :return: 
    """
    # ensure that the linked actor still exists; the link attribute is *always* the dbid of the linked
    # actor
    logger.debug("top of process_link")
    try:
        actors_store[link]
    except KeyError as e:
        logger.error("Processing event message for actor {} that does not exist. Quiting".format(link))
        raise e

    # create an execution for the linked actor with message
    exc = Execution.add_execution(link, {'cpu': 0,
                                         'io': 0,
                                         'runtime': 0,
                                         'status': SUBMITTED,
                                         'executor': 'Abaco Event'})
    logger.info("Events processor agent added execution {} for actor {}".format(exc, link))
    d['_abaco_execution_id'] = exc
    logger.debug("sending message to actor. Final message {} and message dictionary: {}".format(msg, d))
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
            msg = "Events processor get an exception trying to process a message. exception: {}; msg: {}".format(e, msg)
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
            logger.info("events processor using abaco_conf_host_path={}".format(os.environ.get('abaco_conf_host_path')))
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
