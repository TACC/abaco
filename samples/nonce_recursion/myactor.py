import getpass
import os
import requests
import sys
from agavepy.actors import get_context, get_client


def main():
    context = get_context()

    # debug
    print("Contents of context:")
    for k,v in context.items():
        print("key: {}. value: {}".format(k,v))
    print("Contents of env: {}".format(os.environ))

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

    # instantiate the agavepy client:
    try:
        ag = get_client()
    except Exception as e:
        print("Got exception {} trying to get client.".format(e))
        sys.exit()

    # check recursion depth and send message:
    if count > 0:
        count -= 1
        tot += 1
        message = {'count': count, 'tot': tot}
        url = '{}/actors/{}/messages'.format(context['_abaco_api_server']

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
