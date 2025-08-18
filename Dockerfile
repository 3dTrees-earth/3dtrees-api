FROM python:3.12

RUN apt-get update && apt-get install && \
    mkdir -p /src

COPY . /src

RUN cd /src && pip install -e .

WORKDIR /src/trees_api
CMD ["python", "server.py"]
