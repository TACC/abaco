from agavepy.actors import get_context, get_client

def main():

    context = get_context()
    ag = get_client()
    try:
        job = ag.jobs.submit(body=context['message_dict'])
        print("jobs {} submitted successfully.".format(job))
    except Exception as e:
        print("Exception submitting job: {}".format(e))

if __name__ == '__main__':
    main()
