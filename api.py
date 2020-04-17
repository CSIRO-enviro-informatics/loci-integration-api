# -*- coding: utf-8 -*-
#
from collections import OrderedDict
from sanic.response import json, text, HTTPResponse
from sanic.request import Request
from sanic.exceptions import ServiceUnavailable
from sanic_restplus import Api, Resource, fields
from functions import get_linksets, get_datasets, get_locations, get_location_is_within, get_location_contains, get_resource, get_location_overlaps_crosswalk, get_location_overlaps, get_at_location, search_location_by_label
from functions_DGGS import find_dggs_by_loci_uri

url_prefix = '/v1'

api_v1 = Api(title="LOCI Integration API",
             version="1.2",
             prefix=url_prefix, doc='/'.join([url_prefix, "doc"]),
             default_mediatype="application/json",
             additional_css="/static/material_swagger.css")
ns = api_v1.default_namespace

TRUTHS = ("t", "T", "1")

@ns.route('/linksets')
class Linkset(Resource):
    """Operations on LOCI Linksets"""

    @ns.doc('get_linksets', params=OrderedDict([
        ("count", {"description": "Number of linksets to return.",
                   "required": False, "type": "number", "format": "integer", "default": 1000}),
        ("offset", {"description": "Skip number of linksets before returning count.",
                    "required": False, "type": "number", "format": "integer", "default": 0}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets all LOCI Linksets"""
        count = int(next(iter(request.args.getlist('count', [1000]))))
        offset = int(next(iter(request.args.getlist('offset', [0]))))
        meta, linksets = await get_linksets(count, offset)
        response = {
            "meta": meta,
            "linksets": linksets,
        }
        return json(response, status=200)


@ns.route('/datasets')
class Dataset(Resource):
    """Operations on LOCI Datasets"""

    @ns.doc('get_datasets', params=OrderedDict([
        ("count", {"description": "Number of datasets to return.",
                   "required": False, "type": "number", "format": "integer", "default": 1000}),
        ("offset", {"description": "Skip number of datasets before returning count.",
                    "required": False, "type": "number", "format": "integer", "default": 0}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets all LOCI Datasets"""
        count = int(next(iter(request.args.getlist('count', [1000]))))
        offset = int(next(iter(request.args.getlist('offset', [0]))))
        meta, datasets = await get_datasets(count, offset)
        response = {
            "meta": meta,
            "datasets": datasets,
        }
        return json(response, status=200)


@ns.route('/locations')
class Location(Resource):
    """Operations on LOCI Locations"""

    @ns.doc('get_locations', params=OrderedDict([
        ("count", {"description": "Number of locations to return.",
                   "required": False, "type": "number", "format": "integer", "default": 1000}),
        ("offset", {"description": "Skip number of locations before returning count.",
                    "required": False, "type": "number", "format": "integer", "default": 0}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets all LOCI Locations"""
        count = int(next(iter(request.args.getlist('count', [1000]))))
        offset = int(next(iter(request.args.getlist('offset', [0]))))
        meta, locations = await get_locations(count, offset)
        response = {
            "meta": meta,
            "locations": locations,
        }
        return json(response, status=200)


@ns.route('/resource')
class _Resource(Resource):
    """Operations on LOCI Resource"""

    @ns.doc('get_resource', params=OrderedDict([
        ("uri", {"description": "Target LOCI Location/Feature URI",
                 "required": True, "type": "string"}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets a LOCI Resource"""
        resource_uri = str(next(iter(request.args.getlist('uri'))))
        resource = await get_resource(resource_uri)
        return json(resource, status=200)




## The following are non-standard usage of REST/Swagger.
## These are function routes, not resources. But we still define them as an API resource,
## so that they get a GET endpoint and they get auto-documented.


ns_loc_func = api_v1.namespace(
    "loc-func", "Location Functions",
    api=api_v1,
    path='/location/',
)
@ns_loc_func.route('/within')
class Within(Resource):
    """Function for location is Within"""

    @ns.doc('get_location_within', params=OrderedDict([
        ("uri", {"description": "Target LOCI Location/Feature URI",
                    "required": True, "type": "string"}),
        ("count", {"description": "Number of locations to return.",
                   "required": False, "type": "number", "format": "integer", "default": 1000}),
        ("offset", {"description": "Skip number of locations before returning count.",
                    "required": False, "type": "number", "format": "integer", "default": 0}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets all LOCI Locations that this target LOCI URI is within"""
        count = int(next(iter(request.args.getlist('count', [1000]))))
        offset = int(next(iter(request.args.getlist('offset', [0]))))
        target_uri = str(next(iter(request.args.getlist('uri'))))
        meta, locations = await get_location_is_within(target_uri, count, offset)
        response = {
            "meta": meta,
            "locations": locations,
        }
        return json(response, status=200)

@ns_loc_func.route('/contains')
class Contains(Resource):
    """Function for location Contains"""

    @ns.doc('get_location_contains', params=OrderedDict([
        ("uri", {"description": "Target LOCI Location/Feature URI",
                    "required": True, "type": "string"}),
        ("count", {"description": "Number of locations to return.",
                   "required": False, "type": "number", "format": "integer", "default": 1000}),
        ("offset", {"description": "Skip number of locations before returning count.",
                    "required": False, "type": "number", "format": "integer", "default": 0}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets all LOCI Locations that this target LOCI URI contains"""
        count = int(next(iter(request.args.getlist('count', [1000]))))
        offset = int(next(iter(request.args.getlist('offset', [0]))))
        target_uri = str(next(iter(request.args.getlist('uri'))))
        meta, locations = await get_location_contains(target_uri, count, offset)
        response = {
            "meta": meta,
            "locations": locations,
        }
        return json(response, status=200)

@ns_loc_func.route('/overlaps')
class Overlaps(Resource):
    """Function for location Overlaps"""

    @ns.doc('get_location_contains', params=OrderedDict([
        ("uri", {"description": "Target LOCI Location/Feature URI",
                 "required": True, "type": "string"}),
        ("areas", {"description": "Include areas of overlapping features in m2",
                   "required": False, "type": "boolean", "default": False}),
        ("proportion", {"description": "Include proportion of overlap in percent",
                         "required": False, "type": "boolean", "default": False}),
        ("contains", {"description": "Include locations wholly contained in this feature",
                        "required": False, "type": "boolean", "default": False}),
        ("within", {"description": "Include features this location is wholly within",
                    "required": False, "type": "boolean", "default": False}),
        ("output_type", {"description": "Restrict output uris to specified fully qualified uri",
                    "required": False, "type": "string", "default": ''}),
        ("crosswalk", {"description": "Find overlaps event across different spatial hierarchies, some other parameters are ignored: contained, within are all set to true and paging is not currently implemented",
                    "required": False, "type": "boolean", "default": False}),
        ("count", {"description": "Number of locations to return.",
                   "required": False, "type": "number", "format": "integer", "default": 1000}),
        ("offset", {"description": "Skip number of locations before returning count.",
                    "required": False, "type": "number", "format": "integer", "default": 0}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets all LOCI Locations that this target LOCI URI overlaps with\n
        Note: count and offset do not currently work properly on /overlaps """
        count = int(next(iter(request.args.getlist('count', [1000]))))
        offset = int(next(iter(request.args.getlist('offset', [0]))))
        target_uri = str(next(iter(request.args.getlist('uri'))))
        if 'output_type'  in request.args:
            output_featuretype_uri = str(next(iter(request.args.getlist('output_type'))))
        else:
            output_featuretype_uri = None
        include_areas = str(next(iter(request.args.getlist('areas', ['false']))))
        include_proportion = str(next(iter(request.args.getlist('proportion', ['false']))))
        include_contains = str(next(iter(request.args.getlist('contains', ['false']))))
        include_within = str(next(iter(request.args.getlist('within', ['false']))))
        crosswalk = str(next(iter(request.args.getlist('crosswalk', ['false']))))
        include_areas = include_areas[0] in TRUTHS
        include_proportion = include_proportion[0] in TRUTHS
        include_contains = include_contains[0] in TRUTHS
        include_within = include_within[0] in TRUTHS
        crosswalk = crosswalk[0] in TRUTHS
        if crosswalk:
            include_within = False
            meta, overlaps = await get_location_overlaps_crosswalk(target_uri, output_featuretype_uri, include_areas, include_proportion, include_within,
                                                        include_contains, count, offset)
        else:
            meta, overlaps = await get_location_overlaps(target_uri, output_featuretype_uri, include_areas, include_proportion, include_within,
                                                        include_contains, None, count, offset)

        response = {
            "meta": meta,
            "overlaps": overlaps,
        }
        return json(response, status=200)


@ns_loc_func.route('/find_at_location')
class find_at_location(Resource):
    """Function for location find by point"""

    @ns.doc('get_location_contains', params=OrderedDict([
        ("loci_type", {"latitude": "Loci location type to query, can be 'any', 'mb' for meshblocks or 'cc' for contracted catchments",
                 "required": False, "type": "string", "default":"any"}),
        ("lat", {"latitude": "Query point latitude",
                 "required": True, "type": "number", "format": "float"}),
        ("lon", {"longitude": "Query point longitude",
                   "required": False, "type": "number", "format": "float"}),
        ("count", {"description": "Number of locations to return.",
                   "required": False, "type": "number", "format": "integer", "default": 1000}),
        ("offset", {"description": "Skip number of locations before returning count.",
                    "required": False, "type": "number", "format": "integer", "default": 0}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Gets all LOCI Locations that this target LOCI URI overlaps with\n
        Note: count and offset do not currently work properly on /overlaps """
        count = int(next(iter(request.args.getlist('count', [1000]))))
        offset = int(next(iter(request.args.getlist('offset', [0]))))
        lon = float(next(iter(request.args.getlist('lon', None))))
        lat = float(next(iter(request.args.getlist('lat', None))))
        loci_type = str(next(iter(request.args.getlist('loci_type', 'mb'))))
        meta, locations = await get_at_location(lat, lon, loci_type, count, offset)
        response = {
            "meta": meta,
            "locations": locations,
        }

        return json(response, status=200)

@ns_loc_func.route('/find-by-label')
class Search(Resource):
    """Function for finding a LOCI location by label"""

    @ns.doc('find_location_by_label', params=OrderedDict([
        ("query", {"description": "Search query for label",
                    "required": True, "type": "string"}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Calls search engine to query LOCI Locations by label"""
        query = str(next(iter(request.args.getlist('query'))))
        result = await search_location_by_label(query)
        response = result
        return json(response, status=200)

@ns_loc_func.route('/to-DGGS')
class to_DGGS(Resource):
    """Function for finding an array of DGGS cells by a loci uri"""

    @ns.doc('find_dggs_by_loci_uri', params=OrderedDict([
        ("uri", {"description": "Search DGGS cells by loci uri",
                    "required": True, "type": "string"}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Calls search engine to query LOCI Locations by label"""
        print(request.args)
        query = str(next(iter(request.args.getlist('uri'))))
        result = await find_dggs_by_loci_uri(query)
        return json(result, status=200)

@ns_loc_func.route('/find-at-DGGS-cell')
class find_at_DGGS_cell(Resource):
    """Function for finding an array of Loci-i Features by a DGGS cell ID"""

    @ns.doc('find_loci-features_by_dggs', params=OrderedDict([
        ("dggs_cell", {"description": "Search loci features by loci uri",
                    "required": True, "type": "string"}),
    ]), security=None)
    async def get(self, request, *args, **kwargs):
        """Calls search engine to query LOCI Locations by label"""
        query = str(next(iter(request.args.getlist('query'))))
        result = await search_location_by_label(query)
        response = result
        return json(response, status=200)
