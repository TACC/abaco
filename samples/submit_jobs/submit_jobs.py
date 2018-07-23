from agavepy.actors import get_context, get_client

def main():

    context = get_context()
    try:
        job = ag.jobs.submit(body=context['msssage_dict'])
        print("jobs {} submitted successfully.".format(job))
    except Exception as e:
        print("Exception submitting job: {}".format(e))

if __name__ == '__main__':
    main()
