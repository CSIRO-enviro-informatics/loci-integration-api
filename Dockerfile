# Base image provided by sanic
FROM sanicframework/sanic:20.12.2 AS base

WORKDIR /usr/src/app

# Download some dependencies
# including the postgresql libs
RUN apk update && \
    apk add postgresql-dev &&\
    apk add --no-cache git

# Move all the files over
COPY . .

# get setuptools
RUN pip install -U setuptools pip

# Get all requirements and install gunicorn for deployment
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install --no-cache-dir gunicorn

# Deprecated launch
#ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "--port", "8080", "--log-level", "warning", "--no-access-log", "--factory", "app:create_app"]
#CMD ["gunicorn","app:app", "--bind", "0.0.0.0:8080", "--worker-class", "sanic.worker.GunicornWorker"]