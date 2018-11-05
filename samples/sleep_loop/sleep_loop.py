import time

from agavepy.actors import get_context

def main():

    ITERATIONS_DEFAULT = 6
    SLEEP_DEFAULT = 10
    context = get_context()
    try:
        msg_dict = context['message_dict']
    except KeyError as e:
        print("KeyError getting message_dict".format(e))
        msg_dict = None

    iterations = ITERATIONS_DEFAULT
    sleep_time_s =  SLEEP_DEFAULT

    if msg_dict:
        try:
            iterations = int(msg_dict.get('iterations', ITERATIONS_DEFAULT))
            sleep_time_s =  int(msg_dict.get('sleep', SLEEP_DEFAULT))
        except Exception as e:
            print("Got exception {} trying to read iterations and sleep from msg_dict".format(e))

    for i in range(iterations):
        print("starting iteration {}".format(i))
        time.sleep(sleep_time_s)

    print("sleep_loop completed.")

if __name__ == '__main__':
    main()
