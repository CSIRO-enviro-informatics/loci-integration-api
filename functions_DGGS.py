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

# Mapping linked data base uri to loci data type and DGGS columns
DGGS_COLUMN_LOOKUP = {
       "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel1": ["asgs16_sa1", "sa1_main16"],
       "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel2":[ "asgs16_sa2", "sa2_main16"],
       "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel3": ["asgs16_sa3", "sa3_code16"],
        # "http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel4": ["asgs16_sa4"],
        # "http://linked.data.gov.au/dataset/asgs2016/meshblock": [ "asgs16_mb"],
        "http://linked.data.gov.au/dataset/asgs2016/stateorterritory": ["asgs16_ste", "sta_name"],  # need further mapting from code to name
        # "http://linked.data.gov.au/dataset/asgs2016/significanturbanarea": ["asgs16_sua"],
        # "http://linked.data.gov.au/dataset/asgs2016/indigenousregion": ["asgs16_ireg"],
        # "http://linked.data.gov.au/dataset/asgs2016/indigenouslocation": ["asgs16_iloc"],
        # "http://linked.data.gov.au/dataset/asgs2016/indigenousarea": ["asgs16_iare"],
        # "http://linked.data.gov.au/dataset/asgs2016/remotenessarea": ["asgs16_ra"],
        # "http://linked.data.gov.au/dataset/asgs2016/greatercapitalcitystatisticalarea": ["asgs16_gccsa"],
        # "http://linked.data.gov.au/dataset/asgs2016/urbancentreandlocality": ["asgs16_ucl"],
        # "http://linked.data.gov.au/dataset/asgs2016/sectionofstaterange": ["asgs16_sosr"],
        # "http://linked.data.gov.au/dataset/asgs2016/sectionofstate": ["asgs16_sos"],
        "http://linked.data.gov.au/dataset/asgs2016/localgovernmentarea": ["asgs16_lga", "lga_code19"],
        # "http://linked.data.gov.au/dataset/asgs2016/commonwealthelectoraldivision": ["asgs16_ced", "ced_state"], # need further mapping from code to name
        "http://linked.data.gov.au/dataset/asgs2016/statesuburb": ["asgs16_ssc", "ssc_code16"],
        # "http://linked.data.gov.au/dataset/asgs2016/naturalresourcemanagementregion": ["asgs16_nrmr"],
        # "http://linked.data.gov.au/dataset/geofabric/contractedcatchment": ["geofabric2_1_1_ahgfcontractedcatchment"],
        # "http://linked.data.gov.au/dataset/geofabric/riverregion": ["geofabric2_1_1_riverregion"],
        # "http://linked.data.gov.au/dataset/geofabric/drainagedivision": ["geofabric2_1_1_awradrainagedivision"]
    }
# Mapping loci state type to DGGS sta_name
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
            dggs_column = lookup_value[1]
            uri_value = uri[(len(lookup_key)+1):len(uri)]
            break
    sql = f'select auspix_dggs from MainTable02 where {dggs_column}=\'{uri_value}\''
    #print(sql)
    conn = psycopg2.connect(PG_ENDPOINT)
    db_cursor = conn.cursor()
    res = db_cursor.execute(sql)
    records = db_cursor.fetchall() 
    db_cursor.close()
    dggs_cells = []
    for record in records:
        dggs_cells.append(record[0])
    meta = {
        'count': len(dggs_cells),
        'uri': uri
    }
    return meta, dggs_cells

async def find_at_dggs_cell(dggs_cell):
    """
    Function for finding an array of Loci-i features by a DGGS AUxPIX Cell ID, eg "R6810000005"
    """
    dggs_prefix = 'http://ec2-52-63-73-113.ap-southeast-2.compute.amazonaws.com/AusPIX-DGGS-dataset/ausPIX/'
    #print(PG_ENDPOINT)
    index = dggs_cell.find(dggs_prefix)
    dggs_cell_id = dggs_cell
    if index==0:
        dggs_cell_id = dggs_cell[len(dggs_prefix):len(dggs_cell)]
    sql = f'select \
            sa1_main16, \
            sa2_main16, \
            sa3_code16, \
            lga_code19, \
            ssc_code16 \
            FROM MainTable02 WHERE auspix_dggs=\'{dggs_cell_id}\''
    #print(sql)
    conn = psycopg2.connect(PG_ENDPOINT)
    db_cursor = conn.cursor()
    res = db_cursor.execute(sql)
    records = db_cursor.fetchall() 
    db_cursor.close()

    # For each DGGS cell id, only one row will selected
    #print(records)
    locations = []
    if records:
        item = records[0]
        # Manully mapping the selected columns into objects
        sa1_obj = {}
        sa1_obj['uri'] = 'http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel1/'+item[0]
        sa1_obj['datatypeURI'] = 'http://linked.data.gov.au/def/asgs#StatisticalAreaLevel1'
        sa1_obj['dataType'] = 'asgs16_sa1'

        sa2_obj = {}
        sa2_obj['uri'] = 'http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel2/'+item[1]
        sa2_obj['datatypeURI'] = 'http://linked.data.gov.au/def/asgs#StatisticalAreaLevel2'
        sa2_obj['dataType'] = 'asgs16_sa2'

        sa3_obj = {}
        sa3_obj['uri'] = 'http://linked.data.gov.au/dataset/asgs2016/statisticalarealevel3/'+item[2]
        sa3_obj['datatypeURI'] = 'http://linked.data.gov.au/def/asgs#StatisticalAreaLevel3'
        sa3_obj['dataType'] = 'asgs16_sa3'

        lga_obj = {}
        lga_obj['uri'] = 'http://linked.data.gov.au/dataset/asgs2016/localgovernmentarea/'+item[3]
        lga_obj['datatypeURI'] = 'http://linked.data.gov.au/def/asgs#LocalGovernmentArea'
        lga_obj['dataType'] = 'asgs16_lga'

        ssc_obj = {}
        ssc_obj['uri'] = 'http://linked.data.gov.au/dataset/asgs2016/statesuburb/'+item[4]
        ssc_obj['datatypeURI'] = 'http://linked.data.gov.au/def/asgs#StateSuburb'
        ssc_obj['dataType'] = 'asgs16_ssc'

        locations = [sa1_obj, sa2_obj, sa3_obj, lga_obj, ssc_obj]
    meta = {
        'count': len(locations),
        'dggs_cell_id': dggs_cell
    }
    return meta, locations
