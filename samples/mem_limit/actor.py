import sys

from agavepy.actors import get_context

def main():

    # default size, in GB, to test
    print("start of mem_limit")
    SIZE_DEFAULT = 2
    context = get_context()
    try:
        msg_dict = context['message_dict']
    except KeyError as e:
        print("KeyError getting message_dict".format(e))
        msg_dict = None

    if msg_dict:
        try:
            size = int(msg_dict.get('size', SIZE_DEFAULT))
        except Exception as e:
            print("Got exception {} trying to read size from msg_dict".format(e))
    if size > 10:
        print("Invalid size parameter - received a value larger than the max value of 10.")
        raise Exception()
    print("mem_limit using size: {}".format(size))
    # try to create a python object of a certain size -
    ONE_G = 1000000000
    factor = size * ONE_G
    try:
        s = factor*'a'
    except MemoryError as e:
        print("got a memory error; e: {}".format(e))
    except Exception as e:
        print("Got a non-MemoryError exception; e: {}".format(e))
    print("actual size of var: {}".format(sys.getsizeof(s)))
    print("mem_limit completed.")

if __name__ == '__main__':
    main()
