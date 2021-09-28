import os
import cloudpickle
from tapipy.actors import get_binary_message, send_python_result

def main():
    raw_message = get_binary_message()
    try:
        m = cloudpickle.loads(raw_message)
    except Exception as e:
        print(f"Got exception: {e} trying to loads raw_message: {raw_message}")
        raise e
    print("Was able to execute cloudpickle.loads: {}".format(m))
    # to support executing functions that make use of relative paths, the submit() call sends the
    # current working directory in the message. here we try to change to that directory if it
    # exists.
    cwd = m.get('cwd')
    if cwd:
        if os.path.isdir(cwd):
            os.chdir(cwd)
    # the function itself should be passed in the message --
    f = m.get('func')
    if not f:
        print("Error - function attribute required. Got: {}".format(m))
        raise Exception
    # args and kwargs for the function are also passed in the message --
    args = m.get('args')
    kwargs = m.get('kwargs')
    # with the message parts extracted, try to execute the function.
    try:
        result = f(*args, **kwargs)
    except Exception as e:
        print("Got exception trying to call f: {}. Exception: {}".format(f, e))
    # send the result back to Abaco --
    send_python_result(result)
    print("result: {}".format(result))


if __name__ == '__main__':
    main()
