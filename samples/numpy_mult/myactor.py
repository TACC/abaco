import numpy as np
from agavepy.actors import get_context, get_client, send_python_result

def get_message(context):
    """
    :return:Get the message out of the context.
    """
    try:
        message = context['message_dict']
    except Exception as e:
        print("Got an exception parsing message. Aborting. Exception: {}".format(e))
        message = {}
    return message

def f(std_dev, size):
    A = np.random.normal(0, std_dev, (size, size))
    B = np.random.normal(0, std_dev, (size, size))
    C = np.dot(A, B)
    r = np.linalg.eig(C)[0]
    return r

def main():
    print("top of main")
    context = get_context()
    message = get_message(context)
    std_dev = message.get('std_dev', 100)
    size = message.get('size', 4096)
    try:
        std_dev = int(std_dev)
    except:
        std_dev = 100
    try:
        size = int(size)
    except:
        size = 4096
    print("using std_dev {} and size {}".format(std_dev, size))
    r = f(std_dev, size)
    print("result: {}".format(r))
    send_python_result(r)

if __name__ == '__main__':
    main()


