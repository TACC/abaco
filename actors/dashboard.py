import json

from flask import g, request, render_template
import requests

from common.logs import get_logger
logger = get_logger(__name__)

from models import dict_to_camel, display_time


def dashboard():
    # default to using the local instance
    try:
        jwt = g.jwt
    except AttributeError:
        error = "JWT mising. context: {}".format(dir(g))
        return render_template('dashboard.html',
                               actors=[],
                               jwt="",
                               jwt_header="",
                               base_url="",
                               url="",
                               error=error)

    jwt_header = g.jwt_header_name
    base_url = 'http://172.17.0.1:8000'
    url = "{}/admin/actors".format(base_url)
    error = None
    actors = None
    logger.info("jwt_header from context: {}".format(jwt_header))
    logger.debug("jwt from context: {}".format(jwt))
    logger.info("url: {}".format(url))
    if request.method == 'POST':
        logger.info("validating post params.")
        # validate POST parameters
        form_base_url = request.form.get('base_url')
        form_jwt_header = request.form.get('jwt_header')
        form_jwt = request.form.get('jwt')
        if not form_base_url:
            logger.info("Empty base url.")
            error = 'The Base URL is required.'
        elif not form_jwt_header:
            logger.info("Empty JWT header.")
            error = "The JWT Header is required."
        elif not form_jwt:
            logger.info("Empty JWT.")
            error = 'The JWT is required.'
        else:
            logger.info("Using form data.")
            base_url = form_base_url
            jwt_header = form_jwt_header
            jwt = form_jwt

    if not error:
        # try and make a request to get the actors
        headers = {jwt_header: jwt}
        url = "{}/admin/actors".format(base_url)
        logger.info("Submitting GET to: {}".format(url))
        try:
            rsp = requests.get(url, headers=headers)
        except Exception as e:
            logger.error("Got an exception from /admin/actors. Exception: {}".format(e))
            error = "Unable to retrieve actors: {}".format(e)
            return render_template('dashboard.html',
                                   actors=None,
                                   jwt=jwt,
                                   jwt_header=jwt_header,
                                   base_url=base_url,
                                   error=error)
        if rsp.status_code not in [200, 201]:
            logger.error("Did not get 200 from /admin/actors. Status: {}. content: {}".format(
                rsp.status_code, rsp.content))
            if "message" in rsp:
                msg = rsp.get("message")
            else:
                msg = rsp.content
            error = "Unable to retrieve actors. Error was: {}".format(msg)
        else:
            logger.info("Request to /admin/actors successful.")
            data = json.loads(rsp.content.decode('utf-8'))
            actors_data = data.get("result")
            if not actors_data and request.method == 'POST':
                error = "No actors found."
            else:
                actors = []
                for actor in actors_data:
                    a = dict_to_camel(actor)
                    worker = a.get('worker')
                    if worker:
                        try:
                            a['worker'] = dict_to_camel(worker)
                            a['worker']['lastHealthCheckTime'] = display_time(a['worker'].get('lastHealthCheckTime'))
                            a['worker']['lastExecutionTime'] = display_time(a['worker'].get('lastExecutionTime'))
                        except KeyError as e:
                            logger.error("Error pulling worker data from admin api. Exception: {}".format(e))
                    else:
                        a['worker'] = {'lastHealthCheckTime': '',
                                       'lastExecutionTime': '',
                                       'id': '',
                                       'status': ''}
                    logger.info("Adding actor data after converting to camel: {}".format(a))
                    a['createTime'] = display_time(a.get('createTime'))
                    a['lastUpdateTime'] = display_time(a.get('lastUpdateTime'))
                    actors.append(a)

    return render_template('dashboard.html',
                           actors=actors,
                           jwt=jwt,
                           jwt_header=jwt_header,
                           base_url=base_url,
                           url=url,
                           error=error)
