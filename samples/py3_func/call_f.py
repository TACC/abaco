import os
import cloudpickle
from agavepy.actors import get_binary_message, send_python_result

def main():
    raw_message = get_binary_message()
    try:
        m = cloudpickle.loads(raw_message)
    except Exception as e:
        print("Got exception: {} trying to loads raw_message: {}. raw_message: {}".format(e, raw_message))
        raise e
    print("Was able to execute cloudpickle.loads: {}".format(m))
    f = m.get('func')
    if not f:
        print("Error - function attribute required. Got: {}".format(m))
        raise Exception
    args = m.get('args')
    kwargs = m.get('kwargs')
    try:
        result = f(*args, **kwargs)
    except Exception as e:
        print("Got exception trying to call f: {}. Exception: {}".format(f, e))
    send_python_result(result)
    print("result: {}".format(result))

if __name__ == '__main__':
    main()
