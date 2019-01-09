from agavepy.actors import get_context, get_client

def main():
    print("executing")
    context = get_context()
    path = context['message_dict'].get('path', '/data/samples')
    system_id = context['message_dict'].get('system_id', 'reactors.storage.sample')
    ag = get_client()
    try:
        fs = ag.files.list(systemId=system_id, filePath=path)
    except Exception as e:
        try:
            print("files error; content: {}".format(e.response.content))
        except Exception as e2:
            print("files original: {}; 2nd: {}".format(e, e2))
    try:
        p = ag.profiles.get()
    except Exception as e:
        try:
            print("profiles error; content: {}".format(e.response.content))
        except Exception as e2:
            print("profiles original: {}; 2nd: {}".format(e, e2))
    try:
        a = ag.apps.list()
    except Exception as e:
        try:
            print("apps error; content: {}".format(e.response.content))
        except Exception as e2:
            print("apps original: {}; 2nd: {}".format(e, e2))

    try:
        print(len(fs))
        print(p)
        print(a)
    except Exception as e:
        print("failure: {}".format(e))
    # print("Files: {}".format(fs))


if __name__ == '__main__':
    main()