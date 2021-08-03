FROM sanicframework/sanic:20.12.2 AS base

WORKDIR /usr/src/app
RUN apk update && \
    apk add postgresql-dev
COPY . .
RUN pip install -U setuptools pip
RUN apk add --no-cache git
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install --no-cache-dir "uvicorn[standard]"

ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "--port", "8080", "--log-level", "warning", "--no-access-log", "--factory", "app:create_app"]

FROM base AS devmode
#RUN pip install --no-cache-dir -r dev-requirements.txt
RUN git -C loci-testdata pull || git clone https://github.com/CSIRO-enviro-informatics/loci-testdata.git
ENTRYPOINT [ "tail", "-f", "/dev/null" ]

FROM base AS localdevmode
RUN apk add --no-cache vim
ENTRYPOINT [ "tail", "-f", "/dev/null" ]


FROM base
