# -*- coding: utf-8 -*-
#
from aiohttp import ClientSession
from config import TRIPLESTORE_CACHE_SPARQL_ENDPOINT
from json import loads


async def query_graphdb_endpoint(sparql, infer=True, same_as=True, limit=1000, offset=0):
    session = query_graphdb_endpoint.session
    if not session:
        query_graphdb_endpoint.session = session = ClientSession()
    args = {
        'query': sparql,
        'infer': 'true' if bool(infer) else 'false',
        'sameAs': 'true' if bool(same_as) else 'false',
        'limit': int(limit),
        'offset': int(offset),
    }
    headers = {
        'Accept': "application/sparql-results+json,*/*;q=0.9",
        'Accept-Encoding': "gzip, deflate",
    }
    resp = await session.request('POST', TRIPLESTORE_CACHE_SPARQL_ENDPOINT, data=args, headers=headers)
    resp_content = await resp.text()
    return loads(resp_content)
query_graphdb_endpoint.session = None


async def get_linksets(count=1000, offset=0):
    """
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    """
    sparql = """\
PREFIX loci: <http://linked.data.gov.au/def/loci#>
SELECT ?l
WHERE {
    ?l a loci:Linkset .
}
"""
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    linksets = []
    if 'results' not in resp:
        return linksets
    bindings = resp['results']['bindings']
    for b in bindings:
        linksets.append(b['l']['value'])
    meta = {
        'count': len(linksets),
        'offset': offset,
    }
    return meta, linksets

async def get_datasets(count=1000, offset=0):
    """
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    """
    sparql = """\
PREFIX dcat: <http://www.w3.org/ns/dcat#>
SELECT ?d
WHERE {
    ?d a dcat:Dataset .
}
"""
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    datasets = []
    if 'results' not in resp:
        return datasets
    bindings = resp['results']['bindings']
    for b in bindings:
        datasets.append(b['d']['value'])
    meta = {
        'count': len(datasets),
        'offset': offset,
    }
    return meta, datasets
