# A basic Docker image for testing Abaco.
# Image: abacosamples/test

FROM python:3-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
add test.sh /test.sh
run chmod +x /test.sh
CMD ["sh", "/test.sh"]
