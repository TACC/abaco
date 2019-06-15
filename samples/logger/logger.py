# This function can be used to test the limits of the Abaco logging facility. It will
# 1. With a JSON object containing {"text": "the text to count", "reducer": "the id of the reducer actor"}
# 2. With a string of text to count (in this case, skips calling the reducer).

import json
from agavepy.actors import get_context, get_client, send_bytes_result

def main():
    context = get_context()
    try:
        length = context['message_dict']['length']
        print("Got JSON: {}".format(context['message_dict']))
    except Exception:
        print("Didnt get JSON, using defaults.")
        length = 100000
    log = 'a'*length
    print(log)


if __name__ == '__main__':
    main()