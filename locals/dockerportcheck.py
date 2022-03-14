import docker
import time
import requests

cli = docker.APIClient('unix://var/run/docker.sock')
dname = 'testin'
cont = cli.create_container(image='nshresth/docdockdocker', name=dname, ports=[5000], host_config=cli.create_host_config(port_bindings={5000: None}))

container = cli.start(container=cont.get('Id'))
idx = 0
while idx<25:
    try:
        port=cli.inspect_container(cont.get('Id'))['NetworkSettings']['Ports']['5000/tcp'][0]['HostPort']
        break
    except:
        idx = idx+1

print(port)
time.sleep(3)
r = requests.get(f'http://localhost:{port}')
print(r.content)
cli.kill(dname)
cli.prune_containers()

