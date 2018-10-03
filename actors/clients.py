"""
Process to generate Agave clients for workers.
"""

import os
import time
import rabbitpy

from agaveflask.auth import get_api_server

from aga import Agave
from auth import get_tenants, get_tenant_verify
from channels import ClientsChannel
from models import Actor, Client, Worker
from errors import ClientException, WorkerException
from stores import actors_store, clients_store

from agaveflask.logs import get_logger
logger = get_logger(__name__)


class ClientGenerator(object):

    def __init__(self):
        self.secret = os.environ.get('_abaco_secret')
        self.ch = ClientsChannel()
        self.credentials = {}
        for tenant in get_tenants():
            self.credentials[tenant] = {'username': os.environ.get('_abaco_{}_username'.format(tenant), ''),
                                        'password': os.environ.get('_abaco_{}_password'.format(tenant), '')}

    def get_agave(self, tenant, actor_owner):
        """
        Generate an agavepy client representing a specific user owning an actor.
        The `actor_owner` should be the username associated with the owner of the actor.
        """
        # these are the credentials of the abaco service account. this account should have the abaco and
        # impersonator roles.
        username = self.credentials[tenant.upper()]['username']
        password = self.credentials[tenant.upper()]['password']
        if username == '' or password == '':
            msg = 'Client service credentials not defined for tenant {}'.format(tenant)
            logger.error(msg)
            raise ClientException(msg)
        api_server = get_api_server(tenant)
        verify = get_tenant_verify(tenant)
        # generate an Agave client set up for admin_password representing the actor owner:
        logger.info("Attempting to generate an agave client.")
        return api_server,\
               Agave(api_server=api_server,
                     username=username,
                     password=password,
                     token_username=actor_owner,
                     verify=verify)


    def run(self):
        """
        Listen to the clients channel for new client and deletion requests. Requests use the put_sync method
        to send an anonymous channel together with the actual client request command.
        """
        while True:
            message, msg_obj = self.ch.get_one()
            # we directly ack messages from the clients channel because caller expects direct reply_to
            msg_obj.ack()
            logger.info("cleintg processing message: {}".format(message))
            anon_ch = message['reply_to']
            cmd = message['value']
            if cmd.get('command') == 'new':
                logger.debug("calling new_client().")
                self.new_client(cmd, anon_ch)
            elif cmd.get('command') == 'delete':
                logger.debug("calling delete_client().")
                self.delete_client(cmd, anon_ch)
            else:
                msg = 'Received invalid command: {}'.format(cmd.get('command'))
                logger.error(msg)
                anon_ch.put({'status': 'error',
                             'message': msg})
            # @TODO -
            # delete the anonymous channel from this thread but sleep first to avoid the race condition.
            time.sleep(1.5)
            logger.info("deleting the anon_ch associated with the clientg message. anon_ch.name: {}".format(anon_ch.name))
            anon_ch.delete()
            logger.debug("anon_ch deleted.")

            # NOT doing this for now -- deleting entire anon channel instead (see above)
            # clean up the anon channel event queue. this is an issue with the
            # channelpy library
            # anon_ch._queue._event_queue.delete()

    def new_client(self, cmd, anon_ch):
        """Main function to process a `new` command message."""
        valid, msg, owner = self.check_new_params(cmd)
        if valid:
            try:
                api_server, key, secret, access_token, refresh_token = self.generate_client(cmd, owner)
            except ClientException as e:
                logger.error("Error generating client: {}".format(e))
                anon_ch.put({'status': 'error',
                             'message': str(e.msg)})
                return None
            logger.debug("Client generated.")
            cl = Client(**{'tenant': cmd['tenant'],
                           'actor_id': cmd['actor_id'],
                           'worker_id': cmd['worker_id'],
                           'client_key': key,
                           'client_name': cmd['worker_id'],
                         })
            clients_store[cl.id] = cl
            logger.info("client generated and stored. client: {}".format(cl))
            self.send_client(api_server, key, secret, access_token, refresh_token, anon_ch)
        else:
            m = 'Invalid command parameters: {}'.format(msg)
            logger.error(m)
            anon_ch.put({'status': 'error',
                         'message': m})

    def generate_client(self, cmd, owner):
        """Generate an Agave OAuth client whose name is equal to the worker_id that will be using said client."""
        api_server, ag = self.get_agave(cmd['tenant'], actor_owner=owner)
        ag.clients.create(body={'clientName': cmd['worker_id']})
        # note - the client generates tokens representing the user who registered the actor
        logger.info("ag.clients.create successful.")
        return api_server,\
               ag.api_key, \
               ag.api_secret, \
               ag.token.token_info['access_token'], \
               ag.token.token_info['refresh_token']


    def send_client(self, api_server, client_id, client_secret, access_token, refresh_token, anon_ch):
        """Send client credentials to a worker on an anonymous channel."""
        logger.info("sending client credentials for client: {} to channel: {}".format(client_id, anon_ch))
        msg = {'status': 'ok',
               'api_server': api_server,
               'client_id': client_id,
               'client_secret': client_secret,
               'access_token': access_token,
               'refresh_token': refresh_token}
        anon_ch.put(msg)

    def check_common(self, cmd):
        """Common check for new and delete client requests."""
        # validate the secret
        if not cmd.get('secret') == self.secret:
            m = 'Invalid secret.'
            logger.error(m)
            return False, m
        # validate tenant
        if not cmd.get('tenant') in get_tenants():
            m = 'Invalid client passed: {}'.format(cmd.get('tenant'))
            logger.error(m)
            return False, m
        logger.debug("common params were valid.")
        return True, ''

    def check_new_params(self, cmd):
        """Additional checks for new client requests."""
        valid, msg = self.check_common(cmd)
        # validate the actor_id
        try:
            actor = Actor.from_db(actors_store[cmd.get('actor_id')])
        except KeyError:
            m = "Unable to look up actor with id: {}".format(cmd.get('actor_id'))
            logger.error(m)
            return False, m, None
        # validate the worker id
        try:
            Worker.get_worker(actor_id=cmd.get('actor_id'), worker_id=cmd.get('worker_id'))
        except WorkerException as e:
            m = "Unable to look up worker: {}".format(e.msg)
            logger.error(m)
            return False, m, None
        logger.debug("new params were valid.")
        return valid, msg, actor.owner

    def check_del_params(self, cmd):
        """Additional checks for delete client requests."""
        valid, msg = self.check_common(cmd)
        if not cmd.get('client_id'):
            m = 'client_id parameter required.'
            logger.error(m)
            return False, m, None
        # It's possible the actor record has been deleted so we need to remove the client based solely on
        # the information on the command.
        # also, agave owner doesn't matter on delete since we are only using the service account (basic auth).
        logger.debug("del params were valid.")
        return valid, msg, 'abaco_service'

    def delete_client(self, cmd, anon_ch):
        """Main function to process a `delete` command message."""
        valid, msg, owner = self.check_del_params(cmd)
        if not valid:
            anon_ch.put({'status': 'error',
                         'message': 'Invalid parameters sent: {}'.format(msg)})
            return None
        try:
            _, ag = self.get_agave(cmd['tenant'], owner)
        except ClientException as e:
            m = 'Could not generate an Agave client: {}'.format(e)
            logger.error(m)
            anon_ch.put({'status': 'error',
                         'message': m})
            return None
        # remove the client from APIM
        try:
            ag.clients.delete(clientName=cmd['worker_id'])
        except Exception as e:
            m = 'Not able to delete client from APIM. Exception: {}'.format(e)
            logger.error(m)
            anon_ch.put({'status': 'error',
                        'message': m})
            return None
        # remove the client from the abaco db
        try:
            Client.delete_client(tenant=cmd['tenant'], client_key=cmd['client_id'])
        except Exception as e:
            m = 'Not able to delete client from abaco db. Exception: {}'.format(e)
            logger.error(m)
            anon_ch.put({'status': 'error',
                        'message': m})
            return None
        logger.info("client deleted successfully.")
        anon_ch.put({'status': 'ok',
                     'message': 'Client deleted.'})


def main():
    # todo - find something more elegant
    idx = 0
    while idx < 3:
        try:
            client_gen = ClientGenerator()
            logger.info("client generator made connection to rabbit, entering main loop")
            client_gen.run()
        except rabbitpy.exceptions.ConnectionException:
            # rabbit seems to take a few seconds to come up
            time.sleep(5)
            idx += 1
    logger.error("clientg could not make connection to rabbitmq. exiting.")

if __name__ == '__main__':
    main()
