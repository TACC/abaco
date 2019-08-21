FROM python:3.7-alpine

RUN pip install requests
ADD webhook.py /code/

CMD ["python", "/code/webhook.py"]
