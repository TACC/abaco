from agavepy.actors import get_context, get_client

def main():
    context = get_context()
    path = context['message_dict'].get('path', '/data/samples')
    system_id = context['message_dict'].get('system_id', 'reactors.storage.sample')
    ag = get_client()
    fs = ag.files.list(systemId=system_id, filePath=path)
    print("Files: {}".format(fs))


if __name__ == '__main__':
    main()