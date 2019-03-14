import getpass
import os
import requests
import sys
from agavepy.actors import get_context, get_client


def get_message(context):
    """
    :return:Get the message out of the context.
    """
    try:
        message = context['message_dict']
    except Exception as e:
        print("Got an exception parsing message. Aborting. Exception: {}".format(e))
    return message

def get_apy_client(context):
    """Instantiate the agavepy client:
    """
    try:
        ag = get_client()
    except Exception as e:
        print("Got exception {} trying to get client.".format(e))
        sys.exit()
    return ag

def create_second_actor(context, ag):
    """
    Creates a second actor whose image is the abacosamples/nonce image.
    :param context:
    :return:
    """
    try:
        rsp = ag.actors.add(body={'image': 'abacosamples/nonce'})
    except Exception as e:
        print("Got exception {} trying to create second actor.".format(e))
        sys.exit()
    return rsp

def create_nonce(context, ag):
    """
    Creates a nonce to this actor for passing to the second actor; the second actor will use the
    nonce to message back to the first actor.
    :param context:
    :return:
    """
    try:
        rsp = ag.actors.addNone()
    except Exception as e:
        print("Got exception {} trying to create nonce.".format(e))
        sys.exit()
    return rsp


def main():
    context = get_context()
    message = get_message(context)
    ag = get_apy_client(context)

    # if the message has a start field, this is an initiation message to the first actor; we need to do
    # some bootstrapping.
    if 'start' in message:
        rsp = create_second_actor(context, ag)
        actor2_id = rsp['id']
        rsp = create_nonce(context, ag)
        nonce_id = rsp['id']
        count = 0
    else:
        # if the message does not have a start field then this message is a
        # get the target actor id out of the message
        actor2_id = message['target_actor_id']
        count = int(message['count'])
        nonce_id = message['nonce_id']

    # get nonce out of the env
    if 'x-nonce' in context:
        nonce = context['x-nonce']
        print("Got nonce: {}".format(nonce))
    else:
        print("Did not get nonce, exiting.")
        sys.exit()

    # get count and tot out of message
    try:
        count = context['message_dict']['count']
        tot = context['message_dict']['tot']
        print("Got JSON. Count: {} tot: {}".format(count, tot))
    except Exception:
        print("Didnt get JSON, aborting.")
        sys.exit()
    try:
        count = int(count)
        tot = int(tot)
    except Exception:
        print("Got bad count or tot data, exiting.")
        sys.exit()

    # check recursion depth and send message:
    if count > 0:
        count -= 1
        tot += 1
        message = {'count': count, 'tot': tot}
        url = '{}/actors/{}/messages'.format(context['_abaco_api_server'])

    # else, we are done counting; do some basic checks:
    print("*** Posix Identity Information ***")
    try:
        print("Posix username: {}".format(getpass.getuser()))
    except Exception as e:
        # it is possible to get an exception trying to look up the username because the uid may not
        # exist there. Swallow it and keep going.
        print("Got an exception trying to look up the username: {}".format(e))

    print("Posix uid: {}".format(os.getuid()))
    print("Posix gid: {}".format(os.getgid()))

    try:
        prof = ag.profiles.get()
        print("This actor was executed by the user with profile: {}".format(prof))
    except Exception as e:
        print("Got an exception trying to get the profile. Exception: {}".format(e))


if __name__ == '__main__':
    main()
