import asyncio
import asyncpg
import datetime
import threading
from functools import wraps
from decimal import Decimal
from errors import ReportableAPIError
from config import TEMPORAL_CONN_STRING, TEMPORAL_PG_DB_NAME


feature_type_to_semantic_table_lookup = {
"https://linked.data.gov.au/def/asgs#LocalGovernmentArea": "semantic_lga",
"https://linked.data.gov.au/def/asgs#CommonwealthElectoralDivision": "semantic_ced",
"https://linked.data.gov.au/def/asgs#MeshBlock": "semantic_mb",
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel1": "semantic_sa1",
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel2": "semantic_sa2",
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel3": "semantic_sa3",
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel4": "semantic_sa4",
"https://linked.data.gov.au/def/asgs#StateElectoralDivision": "semantic_sed"
}

DATEMIN = datetime.datetime.fromtimestamp(0, datetime.timezone.utc).date()
DATEMAX = datetime.date(2999, 12, 31)
STARTJUL2016 = datetime.date(2016,7,1)
ENDJUN2017 = datetime.date(2017,6,30)
STARTJUL2017 = datetime.date(2017,7,1)
ENDJUN2018 = datetime.date(2018,6,30)
STARTJUL2018 = datetime.date(2018,7,1)
ENDJUN2019 = datetime.date(2019,6,30)
STARTJUL2019 = datetime.date(2019,7,1)
ENDMAY2020 = datetime.date(2020,5,31)
STARTJUN2020 = datetime.date(2020,6,1)

feature_type_to_low_table_lookup = {
"https://linked.data.gov.au/def/asgs#LocalGovernmentArea": {(STARTJUL2016,ENDJUN2017):('lga_2016_aust', 'lga_code16'),(STARTJUL2017,ENDJUN2018):('lga_2017_aust', 'lga_code17'), (STARTJUL2018,ENDJUN2019):('lga_2018_aust', 'lga_code18'), (STARTJUL2019,ENDMAY2020):('lga_2019_aust', 'lga_code19'), (STARTJUN2020,None):('lga_2020_aust', 'lga_code20')},
"https://linked.data.gov.au/def/asgs#CommonwealthElectoralDivision": {(STARTJUL2016,ENDJUN2017):('ced_2016_aust', 'ced_name16'),(STARTJUL2017,ENDJUN2018):('ced_2017_aust', 'ced_name17'), (STARTJUL2018,None):('ced_2018_aust', 'ced_name18')},
"https://linked.data.gov.au/def/asgs#MeshBlock": {(STARTJUL2016,None):("mb_2016_aust", 'mb_code16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel1": {(STARTJUL2016,None):("sa1_2016_aust", 'sa1_main16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel2": {(STARTJUL2016,None):("sa2_2016_aust", 'sa2_main16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel3": {(STARTJUL2016,None):("sa3_2016_aust", 'sa3_code16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel4": {(STARTJUL2016,None):("sa4_2016_aust", 'sa4_code16')},
"https://linked.data.gov.au/def/asgs#StateElectoralDivision": {(STARTJUL2016,ENDJUN2017):('sed_2016_aust', 'sed_name16'), (STARTJUL2017,ENDJUN2018):('sed_2017_aust', 'sed_name17'), (STARTJUL2018,ENDJUN2019):('sed_2018_aust', 'sed_name18'), (STARTJUL2019,ENDMAY2020):('sed_2019_aust', 'sed_name19'), (STARTJUN2020,None):('sed_2020_aust', 'sed_name20')}
}

threadlocal = threading.local()

async def get_pg_connection():
    pg_conn_lock = getattr(threadlocal, 'pg_conn_lock', None)
    if pg_conn_lock is None:
        loop = asyncio.get_event_loop()
        pg_conn_lock = asyncio.Lock(loop=loop)
        setattr(threadlocal, 'pg_conn_lock', pg_conn_lock)
    await pg_conn_lock.acquire()
    try:
        pg_pool = getattr(threadlocal, 'pg_pool', None)
        if pg_pool is None:
            pg_pool = await asyncpg.create_pool(TEMPORAL_CONN_STRING, min_size=8, max_size=12)
            setattr(threadlocal, 'pg_pool', pg_pool)
    finally:
        pg_conn_lock.release()
    return await pg_pool.acquire()


async def return_pg_connection(conn):
    pg_conn_lock = getattr(threadlocal, 'pg_conn_lock', None)
    if pg_conn_lock is None:
        raise RuntimeError("Connection lock doesn't exist. Cannot return pg connection.")
    await pg_conn_lock.acquire()
    try:
        pg_pool = getattr(threadlocal, 'pg_pool', None)
        if pg_pool is None:
            raise RuntimeError("Cannot find pg_pool to return this pg connection!")
    finally:
        pg_conn_lock.release()
    return await pg_pool.release(conn)


def with_conn(f):
    async def inner_f(*args, **kwargs):
        maybe_conn = kwargs.pop("conn", None)
        if maybe_conn is None:
            conn = await get_pg_connection()
            return_conn = True
        else:
            conn = maybe_conn
            return_conn = False
        try:
            return await f(*args, **kwargs, conn=conn)
        finally:
            if return_conn:
                await return_pg_connection(conn)
    return wraps(f)(inner_f)

@with_conn
async def get_all_uris_for_feature(feature_uri, at_date=None, limit=100, offset=0, conn=None):
    if at_date is None:
        query = """SELECT uri, feature_type, dataset, tablename, schemaname, "key", code, valid_from, valid_to FROM (
            SELECT uri as match_uri, valid_from as match_valid  FROM public.uris
            WHERE uri = $1
        ) as sq, uris
        WHERE uris.uri LIKE sq.match_uri||'%' AND uris.valid_from >= sq.match_valid
        ORDER BY uris.uri ASC
        LIMIT $2 OFFSET $3"""
        res_task = conn.fetch(query, feature_uri, limit, offset)
    else:
        if isinstance(at_date, datetime.datetime):
            at_date = at_date.date()
        query = """SELECT uri, feature_type, dataset, tablename, schemaname, "key", code, valid_from, valid_to FROM (
            SELECT uri as match_uri, valid_from as match_valid  FROM public.uris
            WHERE uri = $1
        ) as sq, uris
        WHERE uris.uri LIKE sq.match_uri||'%' AND uris.valid_from <= $2 AND (uris.valid_to IS NULL OR uris.valid_to >= $2)
        ORDER BY uris.uri ASC
        LIMIT $3 OFFSET $4"""
        res_task = conn.fetch(query, feature_uri, at_date, limit, offset)
    return await res_task

@with_conn
async def get_feature_pg(feature_uri, feature_type=None, limit=100, offset=0, conn=None):
    if feature_type is None:
        raise RuntimeError("Lookup feature without feature_type is currently disabled (needs a manual lookup)")
    else:
        table = feature_type_to_semantic_table_lookup.get(feature_type, None)
    if table is None:
        raise RuntimeError("No table known for feature type: {}".format(feature_type))
    query = """SELECT * FROM {} WHERE uri = $1
    ORDER BY uri ASC LIMIT $2 OFFSET $3""".format(table)
    res = await conn.fetch(query, feature_uri, limit, offset)
    return res

@with_conn
async def impl_intersect_features(target_ft, target_table, target_col, source_ft, source_table, source_col, source_id, operation=None, limit=1000, offset=0, conn=None):
    if operation is None or operation == 'intersects':
        operation = "st_intersects(sml.wkb_geometry, big.wkb_geometry)"
    elif operation == 'contains':
        operation = "st_within(sml.wkb_geometry, big.wkb_geometry)"
    elif operation == 'within':
        operation = "st_within(big.wkb_geometry, sml.wkb_geometry)"

    query = """SELECT uris.uri, sml.{0} FROM {1} as sml INNER JOIN {2} as big ON sml.wkb_geometry&&big.wkb_geometry
    INNER JOIN uris ON uris.tablename = $1 AND uris.key = $2 AND uris.feature_type = $3 AND uris.code::text=sml.{0}::text
    WHERE big.{3} = $4 AND {4} LIMIT $5 OFFSET $6""".format(target_col, target_table, source_table, source_col, operation)  # ORDER BY uris.uri ASC <-- this order_by increases search time substantially
    return await conn.fetch(query, target_table, target_col, target_ft, source_id, limit, offset)


def low_table_lookup_at_date(feature_type, at_date):
    defs = feature_type_to_low_table_lookup.get(feature_type, None)
    if defs is None or len(defs) < 1:
        return None
    ordered_defs = reversed(sorted(defs, key=lambda x: x[0]))
    for (start_date,end_date) in ordered_defs:
        if start_date <= at_date and (end_date is None or end_date >= at_date):
            return defs[(start_date, end_date)]
    return None

def low_table_lookup_between_dates(feature_type, from_date, to_date):
    defs = feature_type_to_low_table_lookup.get(feature_type, None)
    if defs is None or len(defs) < 1:
        return []
    ordered_defs = reversed(sorted(defs, key=lambda x: x[0]))
    out_defs = []
    for (start_date,end_date) in ordered_defs:
        if start_date <= to_date and (end_date is None or end_date >= from_date):
            out_defs.append(defs[(start_date, end_date)])
    return out_defs

@with_conn
async def intersect_at_time(from_uri, target_type, at_time, to_time=None, operation=None, limit=1000, offset=0, conn=None):
    if isinstance(at_time, datetime.datetime):
        at_date = at_time.date()
    else:
        at_date = at_time
    if to_time is None:
        to_date = at_date
    elif isinstance(to_time, datetime.datetime):
        to_date = to_time.date()
    else:
        to_date = to_time
    target_table = low_table_lookup_at_date(target_type, to_date)
    if target_table is None:
        raise ReportableAPIError("No table lookup available for FeatureType {} at date {}".format(target_type, to_date))
    (target_table, target_col) = target_table
    (uri, feature_type, schemaname, src_tablename, key, code, valid_from, valid_to) = await get_feature_deref_at_time(
        from_uri, at_date)
    records = await impl_intersect_features(target_type, target_table, target_col, feature_type, src_tablename, key, code, operation=operation, limit=limit, offset=offset, conn=conn)
    uris = [r['uri'] for r in records]
    return jsonable_list(uris)

@with_conn
async def intersect_over_time(from_uri, target_type, from_time, to_time, operation=None, limit=1000, offset=0, conn=None):
    if isinstance(from_time, datetime.datetime):
        from_date = from_time.date()
    else:
        from_date = from_time

    if isinstance(to_time, datetime.datetime):
        to_date = to_time.date()
    else:
        to_date = to_time
    derefs_list = await get_feature_deref_over_time(from_uri, from_date, to_date, limit=100, offset=0, conn=conn)
    outputs = {}
    for d in derefs_list:
        (uri, feature_type, dataset, src_tablename, schemaname, key, code, t_valid_from, t_valid_to) = d
        if t_valid_to is None:
            t_valid_to = to_date  # valid_to can be None, but to_date won't be
        target_tables = low_table_lookup_between_dates(target_type, t_valid_from, t_valid_to)
        if target_tables is None or len(target_tables) < 1:
            continue
        uris = []
        for t in target_tables:
            (target_table, target_col) = t
            records = await impl_intersect_features(target_type, target_table, target_col, feature_type, src_tablename, key, code, operation=operation, limit=limit, offset=offset, conn=conn)
            uris.extend(r['uri'] for r in records)
        outputs[uri] = uris
    return jsonable_dict(outputs)

@with_conn
async def get_feature_deref_at_time(feature_uri, at_date, conn=None):
    feature_derefs = await get_all_uris_for_feature(feature_uri, at_date, limit=100, offset=0, conn=conn)
    (uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to) = (None, None, None, None, None, None, None, None)
    for r in feature_derefs:
        this_uri, this_s, this_t, this_k, this_c, this_valid_to = (r['uri'], r['schemaname'], r['tablename'], r['key'], r['code'], r['valid_to'])
        if uri is None:
            (temp_uri, temp_feature_type, temp_valid_from, temp_valid_to) = (this_uri, r['feature_type'], r['valid_from'], this_valid_to)
        else:
            this_len = len(this_uri)
            u_len = len(uri)
            # TODO: This is a crude way of getting the non-temporal URI from the list!
            if this_len > u_len or (this_uri > uri):
                (temp_uri, temp_feature_type, temp_valid_from, temp_valid_to) = (this_uri, r['feature_type'], r['valid_from'], this_valid_to)
            else:
                continue
        if schemaname is None:
            (uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to) =\
                (temp_uri, temp_feature_type, this_s, this_t, this_k, this_c, temp_valid_from, temp_valid_to)
        else:
            if valid_to is not None and temp_valid_to is None:
                # We already have a temporal_uri, don't look at the non-temporal
                continue
            elif valid_to is None and temp_valid_to is not None:
                # We already have a non-temporal uri, override it with the temporal one
                (uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to) = \
                    (temp_uri, temp_feature_type, this_s, this_t, this_k, this_c, temp_valid_from, temp_valid_to)
            elif valid_to is None and temp_valid_to is None and temp_valid_from >= valid_from:
                # This is a more recent version of the same, use this
                (uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to) = \
                    (temp_uri, temp_feature_type, this_s, this_t, this_k, this_c, temp_valid_from, temp_valid_to)
            else:
                if schemaname != this_s or tablename != this_t or key != this_k or code != this_c:
                    raise ReportableAPIError("Got multiple different features for a single point in time.")
    if None in (uri, feature_type, schemaname, tablename, key, code):
        raise ReportableAPIError("Cannot find a valid record for feature {} at date {}".format(feature_uri, at_date))
    return (uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to)


@with_conn
async def get_feature_at_time(feature_uri, at_time, conn=None):
    if isinstance(at_time, datetime.datetime):
        at_date = at_time.date()
    else:
        at_date = at_time
    (uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to) = await get_feature_deref_at_time(feature_uri, at_date, conn=conn)
    res = await get_feature_pg(uri, feature_type, conn=conn)
    #Assume we've got just one!
    return jsonable_dict(res[0])

@with_conn
async def get_feature_deref_over_time(feature_uri, from_date=DATEMIN, to_date=DATEMAX, limit=100, offset=0, conn=None):
    if conn is None:
        conn = await get_pg_connection()
    derefs = await get_all_uris_for_feature(feature_uri, at_date=None, conn=conn, limit=limit, offset=offset)
    res_list = []
    for d in derefs:
        (uri, feature_type, dataset, tablename, schemaname, key, code, valid_from, valid_to) = d
        if valid_from <= to_date and (valid_to is None or valid_to >= from_date):
            res_list.append(d)
    return res_list

@with_conn
async def get_feature_over_time(feature_uri, from_time=DATEMIN, to_time=DATEMAX, limit=100, offset=0, conn=None):
    if isinstance(from_time, datetime.datetime):
        from_date = from_time.date()
    else:
        from_date = from_time
    if isinstance(to_time, datetime.datetime):
        to_date = to_time.date()
    else:
        to_date = to_time
    deref_list = await get_feature_deref_over_time(feature_uri, from_date, to_date, conn=conn, limit=limit, offset=offset)
    res_list = []
    for d in deref_list:
        (uri, feature_type, dataset, tablename, schemaname, key, code, valid_from, valid_to) = d
        res = await get_feature_pg(uri, feature_type, conn=conn)
        res_list.append(d)
    return jsonable_list(res_list)

@with_conn
async def impl_get_geometry(feature_uri, tablename, key, code, format=None, conn=None):
    if format in (None, "geojson", "application/json", "text/json"):
        operation = "ST_AsGeoJSON"
    elif format in ("text", "wkt", "ewkt", "text/plain", "application/text", "text/wkt", "application/wkt"):
        operation = "ST_ASEWKT"
    else:
        raise ReportableAPIError("Unknown geometry format: {}".format(format))
    query = """SELECT {}(wkb_geometry) as geom FROM {} WHERE {} = $1 LIMIT 1""".format(operation, tablename, key)
    return await conn.fetch(query, code)

@with_conn
async def get_geometry_at_time(feature_uri, format, at_time=None, conn=None):
    res_uri = ""
    tablename, key, code = (None, None, None)
    if at_time is not None:
        if isinstance(at_time, datetime.datetime):
            at_date = at_time.date()
        else:
            at_date = at_time
        d = await get_feature_deref_at_time(feature_uri, at_date, conn=conn)
        (res_uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to) = d
    else:
        deref_list = await get_feature_deref_over_time(feature_uri, conn=conn)

        for d in deref_list:
            this_uri = d[0]
            this_len = len(this_uri)
            if this_len > len(res_uri) or (this_len == len(res_uri) and this_uri > res_uri):  # TODO: This is a crude way of getting the latest temporal version
                (res_uri, feature_type, dataset, tablename, schemaname, key, code, valid_from, valid_to) = d
    if None in (tablename, key, code):
        raise ReportableAPIError("Cannot find a table to get geometry from for URI: {}".format(feature_uri))
    res = await impl_get_geometry(feature_uri, tablename, key, code, format=format, conn=conn)
    if res is None or len(res) < 1:
        raise ReportableAPIError("Cannot get geometry for URI: {}".format(feature_uri))
    return res[0]['geom']

@with_conn
async def impl_text_search(feature_type, find_text, col="name", conn=None):
    sem_table = feature_type_to_semantic_table_lookup[feature_type]
    query = """SELECT uri, {0} FROM {1} WHERE LOWER("{0}") LIKE LOWER('%'||$1||'%');""".format(col, sem_table)
    return await conn.fetch(query, find_text)

@with_conn
async def impl_point_search(feature_type, lat, lon, geom_col="wkb_geometry", srid="4326", count=1000, offset=0, conn=None):
    poly = "SRID=4326;POINT({} {})".format(lon, lat)
    low_tables = feature_type_to_low_table_lookup[feature_type]
    rets = []
    for (start_time, end_time), t in low_tables.items():
        table_name, id_col = t
        query = """SELECT uris.uri, {0} as code FROM {1} AS tbl INNER JOIN uris ON uris.code::text=tbl.{0}::text WHERE uris.tablename=$1 AND uris.key=$2 AND ST_Within(ST_Transform(ST_GeomFromEWKT($3), 4283), tbl.{2})""".format(id_col, table_name, geom_col)
        matches = await conn.fetch(query, table_name, id_col, poly)
        for m in matches:
            rets.append((m['uri'], m['code']))
    return rets


async def temporal_get_by_label(find_text, feature_type=None):
    if feature_type is None:
        feature_types = list(feature_type_to_semantic_table_lookup.keys())
    else:
        feature_type_exists = feature_type_to_semantic_table_lookup.get(feature_type, False)
        if not feature_type_exists:
            raise ReportableAPIError("Feature type {} is not known.".format(feature_type))
        feature_types = [feature_type]

    code_types = feature_types.copy()
    name_types = feature_types.copy()
    #these ones don't have a name
    if "https://linked.data.gov.au/def/asgs#StatisticalAreaLevel1" in name_types:
        name_types.remove("https://linked.data.gov.au/def/asgs#StatisticalAreaLevel1")
    if "https://linked.data.gov.au/def/asgs#MeshBlock" in name_types:
        name_types.remove("https://linked.data.gov.au/def/asgs#MeshBlock")
    jobs = [impl_text_search(n, find_text, col="name") for n in name_types] #Don't pass conn to these
    jobs.extend([impl_text_search(c, find_text, col="code") for c in code_types])
    responses = await asyncio.gather(*jobs)
    resp_pairs = {}
    resp = []
    for r1 in responses:
        if len(r1) < 1:
            continue
        for r2 in r1:
            ruri, rname  = r2
            if ruri not in resp_pairs:
                resp_pairs[ruri] = rname
    for u,m in resp_pairs.items():
        resp.append({"uri": u, "match": m})
    return jsonable_list(resp)


async def temporal_get_by_point(lat, lon, feature_type=None, crs="4326", count=1000, offset=0):
    if feature_type is None or feature_type in ("any", "*", ""):
        feature_types = list(feature_type_to_low_table_lookup.keys())
    else:
        feature_type_exists = feature_type_to_low_table_lookup.get(feature_type, False)
        if not feature_type_exists:
            raise ReportableAPIError("Feature type {} is not known.".format(feature_type))
        feature_types = [feature_type]
    jobs = [impl_point_search(f, lat, lon, srid=crs, count=count, offset=offset) for f in feature_types]
    responses = await asyncio.gather(*jobs)
    resp_pairs = {}
    for r1 in responses:
        if len(r1) < 1:
            continue
        for r2 in r1:
            ruri, rcode = r2
            if ruri not in resp_pairs:
                resp_pairs[ruri] = rcode
    resp = []
    for u,m in resp_pairs.items():
        resp.append({"uri": u, "code": m})
    return jsonable_list(resp)


def jsonable_dict(ft, recursion=9):
    out_dict = {}
    if recursion <= 0:
        return out_dict
    for key,val in ft.items():
        if isinstance(val, (list, tuple)):
            val = jsonable_list(val, recursion=recursion - 1)
        elif isinstance(val, asyncpg.Record):
            val = jsonable_dict(dict(val), recursion=recursion - 1)
        elif isinstance(val, dict):
            val = jsonable_dict(val, recursion=recursion - 1)
        elif isinstance(val, Decimal):
            val = str(val)
        elif isinstance(val, (datetime.datetime,datetime.date)):
            val = val.isoformat()
        out_dict[key] = val
    return out_dict


def jsonable_list(ls, recursion=9):
    out_list = []
    if recursion <= 0:
        return out_list
    for it in ls:
        if isinstance(it, dict):
            it = jsonable_dict(it, recursion=recursion - 1)
        elif isinstance(it, asyncpg.Record):
            it = jsonable_dict(dict(it), recursion=recursion - 1)
        elif isinstance(it, (list, tuple)):
            it = jsonable_list(it, recursion=recursion - 1)
        elif isinstance(it, Decimal):
            it = str(it)
        elif isinstance(it, (datetime.datetime,datetime.date)):
            it = it.isoformat()
        out_list.append(it)
    return out_list

async def main():
    # res = await get_all_uris_for_feature('https://linked.data.gov.au/dataset/asgs/commonwealthelectoraldivision/101', datetime(2020, 1, 1))
    # print(res)
    #res = await get_feature_at_time('https://linked.data.gov.au/dataset/asgs/commonwealthelectoraldivision/101', datetime(2020, 1, 1))
    res = await intersect_at_time('https://linked.data.gov.au/dataset/asgs/commonwealthelectoraldivision/101', "https://linked.data.gov.au/def/asgs#StatisticalAreaLevel2", datetime(2020, 1, 1))
    print(res)
if __name__ == "__main__":
    l = asyncio.get_event_loop()
    l.run_until_complete(main())

