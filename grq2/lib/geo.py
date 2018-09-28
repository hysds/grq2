import json, requests, types
from pprint import pformat
from shapely.geometry import Polygon, MultiPolygon
import shapely.wkt
import cartopy.crs as ccrs


from grq2 import app


def get_center(bbox):
    """
    Return center (lon, lat) of bbox.
    """

    poly = Polygon(bbox)
    src_proj = ccrs.TransverseMercator()
    tgt_proj = src_proj
    for point in bbox:
        if point[0] == -180. or point[0] == 180.:
            if point[1] > 0: tgt_proj = ccrs.RotatedPole(0., 90.)
            else: tgt_proj = ccrs.RotatedPole(0., -90.)
            break
    multipolygons = tgt_proj.project_geometry(poly, src_proj)
    multipolygons = multipolygons.simplify(10.)
    center_lon = multipolygons.centroid.x
    center_lat = multipolygons.centroid.y
    return center_lon, center_lat
