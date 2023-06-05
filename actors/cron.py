import requests
import datetime

from tapisservice.logs import get_logger
from stores import actors_store
from models import site
from controllers import CronResource
from flask import Flask

logger = get_logger(__name__)
app = Flask(__name__)

def main():
    logger.info("Running Metrics check.")
    with app.app_context():
        CronResource().get()

if __name__ == '__main__':
    main()
