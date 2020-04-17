import asyncio
import asyncpg
import math
import psycopg2
from decimal import Decimal
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientConnectorError
from config import TRIPLESTORE_CACHE_SPARQL_ENDPOINT
from config import ES_ENDPOINT
from config import GEOM_DATA_SVC_ENDPOINT
from config import PG_ENDPOINT
from json import loads

from errors import ReportableAPIError

#Until we have a better way of understanding fundamental units in spatial hierarchies
prefix_base_unit_lookup = {
"linked.data.gov.au/dataset/asgs2016" : "linked.data.gov.au/dataset/asgs2016/meshblock",
"linked.data.gov.au/dataset/geofabric" : "linked.data.gov.au/dataset/geofabric/contractedcatchment",
"linked.data.gov.au/dataset/gnaf-2016-05" : "linked.data.gov.au/dataset/gnaf-2016-05/address"
}

prefix_linkset_lookup = {
"linked.data.gov.au/dataset/asgs2016" : ["http://linked.data.gov.au/dataset/mb16cc", "http://linked.data.gov.au/dataset/addr1605mb16" ],
"linked.data.gov.au/dataset/geofabric" : ["http://linked.data.gov.au/dataset/addrcatch", "http://linked.data.gov.au/dataset/mb16cc" ],
"linked.data.gov.au/dataset/gnaf-2016-05" : [ "http://linked.data.gov.au/dataset/addr1605mb16", "http://linked.data.gov.au/dataset/addrcatch"]
}

resource_type_linkset_lookup = {
"http://linked.data.gov.au/def/asgs" : ["http://linked.data.gov.au/dataset/mb16cc", "http://linked.data.gov.au/dataset/addr1605mb16" ],
"http://linked.data.gov.au/def/geofabric" : ["http://linked.data.gov.au/dataset/addrcatch", "http://linked.data.gov.au/dataset/mb16cc" ],
"http://linked.data.gov.au/def/gnaf" : [ "http://linked.data.gov.au/dataset/addr1605mb16", "http://linked.data.gov.au/dataset/addrcatch"]
}

async def get_linkset_uri(from_uri, output_featuretype_uri):
    '''
    Get the linkset connecting an input uri and an output_featuretype_uri
    '''
    from_uri_compatible_linksets = []
    resource_uri_compatible_linksets = []
    if from_uri is None or output_featuretype_uri is None:
        return None
    for uri_prefix, linksets in prefix_linkset_lookup.items():
        if uri_prefix in from_uri:
            from_uri_compatible_linksets = linksets
    for resource_uri_prefix, linksets in resource_type_linkset_lookup.items():
        if resource_uri_prefix in output_featuretype_uri:
            resource_uri_compatible_linksets = linksets
    results = list(set(resource_uri_compatible_linksets).intersection(from_uri_compatible_linksets))
    if len(results) > 0:
        return results[0]
    else:
        return None


async def get_at_location(lat, lon, loci_type="any", count=1000, offset=0):
    """
    :param lat:
    :type lat: float
    :param lon:
    :type lon: float
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    """
    loop = asyncio.get_event_loop()
    try:
        gds_session = get_at_location.session_cache[loop]
    except KeyError:
        gds_session = ClientSession(loop=loop)
        get_at_location.session_cache[loop] = gds_session
    row = {}
    results = {}
    counter = 0
    params = {
       "_format" : "application/json"
    }
    formatted_resp = {
        'ok': False
    }
    http_ok = [200]
    if loci_type == 'any':
       search_by_latlng_url = GEOM_DATA_SVC_ENDPOINT + "/search/latlng/{},{}".format(lon,lat)
    else:
       search_by_latlng_url = GEOM_DATA_SVC_ENDPOINT + "/search/latlng/{},{}/dataset/{}".format(lon, lat, loci_type)

    try:
        resp = await gds_session.request('GET', search_by_latlng_url, params=params)
        resp_content = await resp.text()
        if resp.status not in http_ok:
            formatted_resp['errorMessage'] = "Could not connect to the geometry data service at {}. Error code {}".format(GEOM_DATA_SVC_ENDPOINT, resp.status)
            return formatted_resp
        formatted_resp = loads(resp_content)
        formatted_resp['ok'] = True
    except ClientConnectorError:
        formatted_resp['errorMessage'] = "Could not connect to the geometry data service at {}. Connection error thrown.".format(GEOM_DATA_SVC_ENDPOINT)
        return formatted_resp
    meta = {
        'count': formatted_resp['count'],
        'offset': offset,
    }
    return meta, formatted_resp
get_at_location.session_cache = {}

async def query_es_endpoint(query, limit=10, offset=0):
    """
    Pass the ES query to the endpoint. The endpoint is specified in the config file.

    :param query: the query text
    :type query: str
    :param limit:
    :type limit: int
    :param offset:
    :type offset: int
    :return:
    :rtype: dict
    """
    loop = asyncio.get_event_loop()
    http_ok = [200]
    try:
        session = query_es_endpoint.session_cache[loop]
    except KeyError:
        session = ClientSession(loop=loop)
        query_es_endpoint.session_cache[loop] = session
    args = {
        'q': query
#        'limit': int(limit),
#        'offset': int(offset),
    }

    formatted_resp = {
        'ok': False
    }
    try:
        resp = await session.request('GET', ES_ENDPOINT, params=args)
        resp_content = await resp.text()
        if resp.status not in http_ok:
            formatted_resp['errorMessage'] = "Could not connect to the label search engine. Error code {}".format(resp.status)
            return formatted_resp
        formatted_resp = loads(resp_content)
        formatted_resp['ok'] = True
        return formatted_resp
    except ClientConnectorError:
        formatted_resp['errorMessage'] = "Could not connect to the label search engine. Connection error thrown."
        return formatted_resp
    return formatted_resp
query_es_endpoint.session_cache = {}


async def search_location_by_label(query):
    """
    Query ElasticSearch endpoint and search by label of LOCI locations.
    The query to ES is in the format of http://localhost:9200/_search?q=NSW

    Returns response back from ES as-is.

    :param query: query string for text matching on label of LOCI locations
    :type query: str
    :return:
    :rtype: dict
    """
    resp = await query_es_endpoint(query)

    if ('ok' in resp and resp['ok'] == False):
        return resp

    resp_object = {}
    if 'hits' not in resp:
        return resp_object

    resp_object = resp
    return resp_object

async def find_dggs_by_loci_uri(uri):
    """

    """
    dggs_column = 'sa1_main16'
    uri_value = ""
    for lookup_key, lookup_value in DGGS_COLUMN_LOOKUP.items():
        index = uri.find(lookup_key)
        if index>=0:
            print(index, lookup_key, lookup_value)
            dggs_column = lookup_value
            uri_value = uri[len(lookup_key)+1:-1]
            print(index, uri, uri_value)
            break
    sql = f'select auspix_dggs from public."MainTable01" where {dggs_column}=\'{uri_value}\''
    print(PG_ENDPOINT)
    conn = psycopg2.connect(PG_ENDPOINT)
    db_cursor = conn.cursor()
    res = db_cursor.execute(sql)
    records = db_cursor.fetchall() 
    db_cursor.close()
    result = []
    for record in records:
        result.append(record)
    return result

DGGS_COLUMN_LOOKUP = {
       "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel1": "sa1_main16",
       "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel2": "sa2_main16",
       "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel3": "sa3_code16",
        # "asgs16_sa4": "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel4",
        # "asgs16_mb": "http://linked.data.gov.au/dataset/asgs2016/meshblock",
        "http://linked.data.gov.au/dataset/asgs2016/stateorterritory": "sta_name",  # need further mapting from code to name
        # "asgs16_sua": "http://linked.data.gov.au/dataset/asgs2016/significanturbanarea",
        # "asgs16_ireg": "http://linked.data.gov.au/dataset/asgs2016/indigenousregion",
        # "asgs16_iloc": "http://linked.data.gov.au/dataset/asgs2016/indigenouslocation",
        # "asgs16_iare": "http://linked.data.gov.au/dataset/asgs2016/indigenousarea",
        # "asgs16_ra": "http://linked.data.gov.au/dataset/asgs2016/remotenessarea",
        # "asgs16_gccsa": "http://linked.data.gov.au/dataset/asgs2016/greatercapitalcitystatisticalarea",
        # "asgs16_ucl": "http://linked.data.gov.au/dataset/asgs2016/urbancentreandlocality",
        # "asgs16_sosr": "http://linked.data.gov.au/dataset/asgs2016/sectionofstaterange",
        # "asgs16_sos": "http://linked.data.gov.au/dataset/asgs2016/sectionofstate",
        "http://linked.data.gov.au/dataset/asgs2016/localgovernmentarea": "lga_code19",
        # "asgs16_ced": "http://linked.data.gov.au/dataset/asgs2016/commonwealthelectoraldivision": "ced_state", # need further mapping from code to name
        "http://linked.data.gov.au/dataset/asgs2016/statesuburb": "ssc_code16",
        # "asgs16_nrmr": "http://linked.data.gov.au/dataset/asgs2016/naturalresourcemanagementregion",
        # "geofabric2_1_1_ahgfcontractedcatchment": "http://linked.data.gov.au/dataset/geofabric/contractedcatchment",
        # "geofabric2_1_1_riverregion": "http://linked.data.gov.au/dataset/geofabric/riverregion",
        # "geofabric2_1_1_awradrainagedivision": "http://linked.data.gov.au/dataset/geofabric/drainagedivision"
    }
STATE_TO_CODE = {
    "1": "New South Wales",
    "2": "Victoria",
    "3": "Queensland",
    "4": "South Australia", 
    "5": "Western Australia",
    "6": "Tasmania",
    "7": "Northern Territory",
    "8": "Australian Capital", 
    "9": ''
}

def find_resource_uri(cls, dataset_type, dataset_local_id):
    prefix = cls.DATASET_RESOURCE_BASE_URI_LOOKUP.get(dataset_type)
    if prefix is None:
        return None
    return "{0}/{1}".format(prefix, dataset_local_id)