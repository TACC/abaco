"""
Process to generate Agave clients for workers.
"""

from __init__ import t
from errors import WorkerException
from common.config import conf
from common.logs import get_logger
logger = get_logger(__name__)


def get_delegation_token(sub_tenant, sub_user, access_token_ttl=14400):
    #try:
    #    token_res = t.tokens.create_token(account_type='user',
    #                                      token_tenant_id=conf.service_tenant_id,
    #                                      token_username=conf.service_name,
    #                                      delegation_token=True,
    #                                      delegation_sub_tenant_id=sub_tenant,
    #                                      delegation_sub_username=sub_user,
    #                                      access_token_ttl=access_token_ttl,
    #                                      generate_refresh_token=False,
    #                                      refresh_token_ttl=0,
    #                                      claims={})
    #    return token_res.access_token.access_token
    try:
        return "hello my name is john and I will be you're token for this flight."
    except Exception as e:
        msg = f"Got exception trying to create actor access_token; exception: {e}"
        logger.error(msg)
        raise WorkerException(msg)