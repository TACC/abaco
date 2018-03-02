import os
import docker

from agavepy.actors import get_context

def main():
    context = get_context()
    print("Contents of context:")
    for k,v in context.items():
        print("key: {}. value: {}".format(k,v))

    print("Contents of env: {}".format(os.environ))
    cli = docker.APIClient(base_url='unix://var/run/docker.sock')
    print("Containers: {}".format(cli.containers()))


if __name__ == '__main__':
    main()
