import asyncio
import asyncpg
import datetime
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
"https://linked.data.gov.au/def/asgs#CommonwealthElectoralDivision": {(STARTJUL2016,ENDJUN2017):('ced_2016_aust', 'ced_code16'),(STARTJUL2017,ENDJUN2018):('ced_2017_aust', 'ced_code17'), (STARTJUL2018,None):('ced_2018_aust', 'ced_code18')},
"https://linked.data.gov.au/def/asgs#MeshBlock": {(STARTJUL2016,None):("mb_2016_aust", 'mb_code16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel1": {(STARTJUL2016,None):("sa1_2016_aust", 'sa1_main16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel2": {(STARTJUL2016,None):("sa2_2016_aust", 'sa2_main16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel3": {(STARTJUL2016,None):("sa3_2016_aust", 'sa3_code16')},
"https://linked.data.gov.au/def/asgs#StatisticalAreaLevel4": {(STARTJUL2016,None):("sa4_2016_aust", 'sa4_code16')},
"https://linked.data.gov.au/def/asgs#StateElectoralDivision": {(STARTJUL2016,ENDJUN2017):('sed_2016_aust', 'sed_code16'), (STARTJUL2017,ENDJUN2018):('sed_2017_aust', 'sed_code17'), (STARTJUL2018,ENDJUN2019):('sed_2018_aust', 'sed_code18'), (STARTJUL2019,ENDMAY2020):('sed_2019_aust', 'sed_code19'), (STARTJUN2020,None):('sed_2020_aust', 'sed_code20')}
}


async def make_pg_connection():
    conn = await asyncpg.connect(TEMPORAL_CONN_STRING)
    return conn


async def get_all_uris_for_feature(feature_uri, at_date=None, limit=100, offset=0, conn=None):
    if conn is None:
        conn = await make_pg_connection()
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


async def get_feature_pg(feature_uri, feature_type=None, limit=100, offset=0, conn=None):
    if feature_type is None:
        raise RuntimeError("Lookup feature without feature_type is currently disabled (needs a manual lookup)")
    else:
        table = feature_type_to_semantic_table_lookup.get(feature_type, None)
    if table is None:
        raise RuntimeError("No table known for feature type: {}".format(feature_type))
    if conn is None:
        conn = await make_pg_connection()
    query = """SELECT * FROM {} WHERE uri = $1
    ORDER BY uri ASC LIMIT $2 OFFSET $3""".format(table)
    res = await conn.fetch(query, feature_uri, limit, offset)
    return res


async def impl_intersect_features(target_ft, target_table, target_col, source_ft, source_table, source_col, source_code, operation=None, limit=1000, offset=0, conn=None):
    if conn is None:
        conn = await make_pg_connection()
    if operation is None or operation == 'intersects':
        operation = "st_intersects(sml.wkb_geometry, big.wkb_geometry)"
    elif operation == 'contains':
        operation = "st_within(sml.wkb_geometry, big.wkb_geometry)"
    elif operation == 'within':
        operation = "st_within(big.wkb_geometry, sml.wkb_geometry)"

    query = """SELECT uris.uri, sml.{0} FROM {1} as sml INNER JOIN {2} as big ON {4}
    INNER JOIN uris ON uris.tablename = $1 AND uris.key = $2 AND uris.feature_type = $3 AND uris.code=sml.{0}
    WHERE big.{3} = $4 LIMIT $5 OFFSET $6""".format(target_col, target_table, source_table, source_col, operation) # ORDER BY uris.uri ASC <-- this order_by increases search time substantially
    return await conn.fetch(query, target_table, target_col, target_ft, source_code, limit, offset)


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
    if conn is None:
        conn = await make_pg_connection()
    (uri, feature_type, schemaname, src_tablename, key, code, valid_from, valid_to) = await get_feature_deref_at_time(
        from_uri, at_date)
    records = await impl_intersect_features(target_type, target_table, target_col, feature_type, src_tablename, key, code, operation=operation, limit=limit, offset=offset, conn=conn)
    uris = [r['uri'] for r in records]
    return jsonable_list(uris)

async def intersect_over_time(from_uri, target_type, from_time, to_time, operation=None, limit=1000, offset=0, conn=None):
    if isinstance(from_time, datetime.datetime):
        from_date = from_time.date()
    else:
        from_date = from_time

    if isinstance(to_time, datetime.datetime):
        to_date = to_time.date()
    else:
        to_date = to_time
    if conn is None:
        conn = await make_pg_connection()
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


async def get_feature_deref_at_time(feature_uri, at_date, conn=None):
    if conn is None:
        conn = await make_pg_connection()
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


async def get_feature_at_time(feature_uri, at_time, conn=None):
    if isinstance(at_time, datetime.datetime):
        at_date = at_time.date()
    else:
        at_date = at_time
    if conn is None:
        conn = await make_pg_connection()
    (uri, feature_type, schemaname, tablename, key, code, valid_from, valid_to) = await get_feature_deref_at_time(feature_uri, at_date, conn=conn)
    res = await get_feature_pg(uri, feature_type, conn=conn)
    #Assume we've got just one!
    return jsonable_dict(res[0])


async def get_feature_deref_over_time(feature_uri, from_date, to_date, conn=None, limit=100, offset=0):
    if conn is None:
        conn = await make_pg_connection()
    derefs = await get_all_uris_for_feature(feature_uri, at_date=None, conn=conn, limit=limit, offset=offset)
    res_list = []
    for d in derefs:
        (uri, feature_type, dataset, tablename, schemaname, key, code, valid_from, valid_to) = d
        if valid_from <= to_date and (valid_to is None or valid_to >= from_date):
            res_list.append(d)
    return res_list


async def get_feature_over_time(feature_uri, from_time, to_time, conn=None, limit=100, offset=0):
    if isinstance(from_time, datetime.datetime):
        from_date = from_time.date()
    else:
        from_date = from_time
    if isinstance(to_time, datetime.datetime):
        to_date = to_time.date()
    else:
        to_date = to_time
    if conn is None:
        conn = await make_pg_connection()
    deref_list = await get_feature_deref_over_time(feature_uri, from_date, to_date, conn=conn, limit=limit, offset=offset)
    res_list = []
    for d in deref_list:
        (uri, feature_type, dataset, tablename, schemaname, key, code, valid_from, valid_to) = d
        res = await get_feature_pg(uri, feature_type, conn=conn)
        res_list.append(d)
    return jsonable_list(res_list)


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

