import requests
import datetime

from stores import actors_store
from controllers import CronResource
from flask import Flask

from agaveflask.logs import get_logger
logger = get_logger(__name__)

app = Flask(__name__)

def main():
    logger.info("Running Cron check.")
    with app.app_context():
        CronResource().get()

if __name__ == '__main__':
    main()