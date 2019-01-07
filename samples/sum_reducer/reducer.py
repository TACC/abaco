import ast
import sys

from agavepy.actors import get_context, update_state


def main():
    context = get_context()
    try:
        count = context['message_dict']['count']
        print("Got JSON. Count: {}".format(count))
    except Exception:
        print("Didnt get JSON, aborting.")
        sys.exit()
    state = context['state']
    print("Type of state: {}".format(type(state)))
    print("Initial state: {}".format(state))
    if type(state) == str:
        try:
            state = ast.literal_eval(state)
        except ValueError:
            state = {}
    else:
        state = {}
    # iterate over all keys in the count
    for k, v in count.items():
        if k in state:
            state[k] += v
        else:
            state[k] = v
    print("Final word count:")
    for k,v in state.items():
        print("{}: {}".format(k, v))
    update_state(state)

if __name__ == '__main__':
    main()