FROM python:3.12

RUN apt-get update && apt-get install && \
    mkdir -p /src

COPY . /src

RUN cd /src && pip install -e . && pip install pytest pytest-asyncio

WORKDIR /src
CMD ["python", "trees_api/server.py"]
