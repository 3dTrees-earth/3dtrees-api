FROM python:3.12

RUN apt-get update && apt-get install && \
    mkdir -p /src

COPY . /src

RUN cd /src && pip install -e . && pip install pytest pytest-asyncio debugpy

ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown
ENV GIT_SHA=$GIT_SHA
ENV BUILD_TIME=$BUILD_TIME

WORKDIR /src
CMD ["python", "trees_api/server.py"]
