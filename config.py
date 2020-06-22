# -*- coding: utf-8 -*-
#
import sys
import os
module = sys.modules[__name__]
CONFIG = module.CONFIG = {}
TRIPLESTORE_CACHE_URL = os.environ.get('TRIPLESTORE_CACHE_URL')
if TRIPLESTORE_CACHE_URL is None or TRIPLESTORE_CACHE_URL == '':
    TRIPLESTORE_CACHE_URL = CONFIG["TRIPLESTORE_CACHE_URL"] = "http://db.loci.cat"
#TRIPLESTORE_CACHE_URL = CONFIG["TRIPLESTORE_CACHE_URL"] = "http://db.loci.cat"
TRIPLESTORE_CACHE_PORT = os.environ.get('TRIPLESTORE_CACHE_PORT')
if TRIPLESTORE_CACHE_PORT is None or TRIPLESTORE_CACHE_PORT == '':
    TRIPLESTORE_CACHE_PORT = CONFIG["TRIPLESTORE_CACHE_PORT"] = "80"
TRIPLESTORE_CACHE_SPARQL_ENDPOINT = CONFIG["TRIPLESTORE_CACHE_SPARQL_ENDPOINT"] = \
    "{}:{}/repositories/loci-cache".format(TRIPLESTORE_CACHE_URL, TRIPLESTORE_CACHE_PORT)

ES_URL = CONFIG["ES_URL"] = "http://elasticsearch"
ES_PORT = CONFIG["ES_ENDPOINT"] = "9200"
ES_ENDPOINT = CONFIG["ES_ENDPOINT"] = \
    "{}:{}/_search".format(ES_URL, ES_PORT)


GEOM_DATA_SVC_ENDPOINT = os.environ.get('GEOM_DATA_SVC_ENDPOINT')
if GEOM_DATA_SVC_ENDPOINT is None or GEOM_DATA_SVC_ENDPOINT == '':
   GEOM_DATA_SVC_ENDPOINT = CONFIG["GEOM_DATA_SVC_ENDPOINT"] = "https://gds.loci.cat"

LOCI_DATATYPES_STATIC_JSON = os.environ.get('LOCI_DATATYPES_STATIC_JSON')
if LOCI_DATATYPES_STATIC_JSON is None or LOCI_DATATYPES_STATIC_JSON == '':
   LOCI_DATATYPES_STATIC_JSON = CONFIG["LOCI_DATATYPES_STATIC_JSON"] = "https://loci.cat/json-ld/loci-types.json"
  

PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DB_NAME = os.environ.get('PG_DB_NAME')
PG_USER = os.environ.get('PG_USER')
PG_PASSWORD = os.environ.get('PG_PASSWORD')
PG_TABLE = os.environ.get('PG_TABLE')

PG_ENDPOINT = f'host={PG_HOST} port={PG_PORT} dbname={PG_DB_NAME} user={PG_USER} password={PG_PASSWORD}'
