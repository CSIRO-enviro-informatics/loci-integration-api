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

async def find_dggs_by_loci_uri(uri):
    """
    Function for finding an array of DGGS cells by a loci uri, eg: http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel1/31503140814
    """
    dggs_column = 'sa1_main16'
    uri_value = ""
    for lookup_key, lookup_value in DGGS_COLUMN_LOOKUP.items():
        index = uri.find(lookup_key)
        if index==0:
            dggs_column = lookup_value
            uri_value = uri[len(lookup_key):len(uri)]
            break
    sql = f'select auspix_dggs from public."MainTable01" where {dggs_column}=\'{uri_value}\''
#     print(sql)
    conn = psycopg2.connect(PG_ENDPOINT)
    db_cursor = conn.cursor()
    res = db_cursor.execute(sql)
    records = db_cursor.fetchall() 
    db_cursor.close()
    result = []
    for record in records:
        result.append(record)
    return result

async def find_at_dggs_cell(dggs_cell):
    """
    Function for finding an array of Loci-i features by a DGGS AUxPIX Cell ID, eg "R6810000005"
    """
    dggs_prefix = 'http://ec2-52-63-73-113.ap-southeast-2.compute.amazonaws.com/AusPIX-DGGS-dataset/ausPIX/'

    index = dggs_cell.find(dggs_prefix)
    dggs_cell_id = dggs_cell
    if index==0:
        dggs_cell_id = dggs_cell[len(dggs_prefix):len(dggs_cell)]
    sql = f'select \
            \'http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel1/\'||sa1_main16 as asgs16_sa1, \
            \'http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel2/\'||sa2_main16 as asgs16_sa2, \
            \'http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel3/\'||sa3_code16 as asgs16_sa3, \
            \'http://linked.data.gov.au/dataset/asgs2016/localgovernmentarea/\'||lga_code19 as asgs16_lga, \
            \'http://linked.data.gov.au/dataset/asgs2016/statesuburb/\'||ssc_code16 as asgs16_ssc \
            FROM public."MainTable01" WHERE auspix_dggs=\'{dggs_cell_id}\''
    # print(sql)
    conn = psycopg2.connect(PG_ENDPOINT)
    db_cursor = conn.cursor()
    res = db_cursor.execute(sql)
    records = db_cursor.fetchall() 
    db_cursor.close()
    result = []
    for item in records:
        obj = {}
        obj['asgs16_sa1'] = item[0]
        obj['asgs16_sa2'] = item[1]
        obj['asgs16_sa3'] = item[2]
        obj['asgs16_lga'] = item[3]
        obj['asgs16_ssc'] = item[4]
        result.append(obj)
    return result
