# This function does a basic word count on a string. There are two ways to call it;
# 1. With a JSON object containing {"text": "the text to count", "reducer": "the id of the reducer actor"}
# 2. With a string of text to count (in this case, skips calling the reducer).

import json
from agavepy.actors import get_context, get_client, send_bytes_result

def main():
    context = get_context()
    try:
        m = context['message_dict']['text']
        reducer = context['message_dict']['reducer']
        print("Got JSON.")
    except Exception:
        print("Didnt get JSON, using raw message.")
        reducer = None
        m = context.raw_message
    wordcount = {}
    for word in m.split():
        w = word.lower()
        if w in wordcount:
            wordcount[w] += 1
        else:
            wordcount[w] = 1
    print("Final word count:")
    for k,v in wordcount.items():
        print("{}: {}".format(k, v))
    print("Sending bytes result")
    send_bytes_result(json.dumps(wordcount).encode())
    if reducer:
        print("Sending message to reducer {}".format(reducer))
        ag = get_client()
        ag.actors.sendMessage(actorId=reducer, body={'message': {'count': wordcount}})
    else:
        print("skipping reducer")

if __name__ == '__main__':
    main()