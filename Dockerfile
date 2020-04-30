FROM sanicframework/sanic:LTS AS base

WORKDIR /usr/src/app
RUN apk update && \
    apk add postgresql-dev
COPY . .
RUN pip install -U setuptools pip
RUN apk add --no-cache git
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "python3", "./app.py" ]

FROM base AS devmode
#RUN pip install --no-cache-dir -r dev-requirements.txt
RUN git -C loci-testdata pull || git clone https://github.com/CSIRO-enviro-informatics/loci-testdata.git
ENTRYPOINT [ "tail", "-f", "/dev/null" ]

FROM base AS localdevmode
RUN apk add --no-cache vim
ENTRYPOINT [ "tail", "-f", "/dev/null" ]


FROM base 
