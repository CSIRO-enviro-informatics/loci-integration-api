# -*- coding: utf-8 -*-
#
import sys
import os
module = sys.modules[__name__]
CONFIG = module.CONFIG = {}
TRIPLESTORE_CACHE_URL = os.environ.get('TRIPLESTORE_CACHE_URL')
if TRIPLESTORE_CACHE_URL is None or TRIPLESTORE_CACHE_URL == '':
    TRIPLESTORE_CACHE_URL = CONFIG["TRIPLESTORE_CACHE_URL"] = "http://db.loci.cat"
TRIPLESTORE_CACHE_PORT = CONFIG["TRIPLESTORE_CACHE_PORT"] = "80"
TRIPLESTORE_CACHE_SPARQL_ENDPOINT = CONFIG["TRIPLESTORE_CACHE_SPARQL_ENDPOINT"] = \
    "{}:{}/repositories/loci-cache".format(TRIPLESTORE_CACHE_URL, TRIPLESTORE_CACHE_PORT)

ES_URL = CONFIG["ES_URL"] = "http://elasticsearch"
ES_PORT = CONFIG["ES_ENDPOINT"] = "9200"
ES_ENDPOINT = CONFIG["ES_ENDPOINT"] = \
    "{}:{}/_search".format(ES_URL, ES_PORT)


GEOM_DATA_SVC_ENDPOINT = CONFIG["GEOM_DATA_SVC_ENDPOINT"] = "https://gds.loci.cat"

PG_HOST = 'location-index.cx6rhroaeuxv.ap-southeast-2.rds.amazonaws.com'
PG_PORT = '5432'
PG_DB_NAME = 'postgres'
PG_USER = 'govreadonly'
PG_PASSWORD = 'lociauspixdggs10'

PG_ENDPOINT = f'host={PG_HOST} port={PG_PORT} dbname={PG_DB_NAME} user={PG_USER} password={PG_PASSWORD}'