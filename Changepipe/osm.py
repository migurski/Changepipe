from sys import stderr
from xml.parsers.expat import ExpatError
from xml.etree.ElementTree import parse
from urllib import urlopen

from shapely.geometry import Point, MultiPoint, Polygon

expiration = 86400

def api_xml(url):
    """
    """
    print >> stderr, ' ', url
    
    return parse(urlopen(url))

def changed_elements(stream):
    """
    """
    changes = parse(stream)
    elements = []
    
    for change in changes.getroot().getchildren():
        if change.tag in ('create', 'modify', 'delete'):
            elements += change.getchildren()
    
    return elements

def remember_node(redis, attrib):
    """
    """
    node_key = 'node-%(id)s' % attrib

    redis.hset(node_key, 'version', attrib['version'])
    redis.hset(node_key, 'lat', attrib['lat'])
    redis.hset(node_key, 'lon', attrib['lon'])
    
    redis.expire(node_key, expiration)

def remember_changeset(redis, attrib):
    """
    """
    changeset_key = 'changeset-%(id)s' % attrib

    redis.hset(changeset_key, 'min_lat', attrib['min_lat'])
    redis.hset(changeset_key, 'min_lon', attrib['min_lon'])
    redis.hset(changeset_key, 'max_lat', attrib['max_lat'])
    redis.hset(changeset_key, 'max_lon', attrib['max_lon'])
    redis.hset(changeset_key, 'user', attrib['user'])
    redis.hset(changeset_key, 'created_at', attrib['created_at'])
    
    redis.expire(changeset_key, expiration)

def changeset_bounds(redis, changeset_key, ask_osm_api):
    """
    """
    minlat, minlon, maxlat, maxlon = [redis.hget(changeset_key, a) for a in 'min_lat min_lon max_lat max_lon'.split()]
    
    if ask_osm_api and (minlat is None or minlon is None or maxlat is None or maxlon is None):
        xml = api_xml('http://api.openstreetmap.org/api/0.6/changeset/%s' % changeset_key[10:])
        change = xml.find('changeset')
        
        if 'min_lat' in change.attrib:
            remember_changeset(redis, change.attrib)
            minlat, minlon = redis.hget(changeset_key, 'min_lat'), redis.hget(changeset_key, 'min_lon')
            maxlat, maxlon = redis.hget(changeset_key, 'max_lat'), redis.hget(changeset_key, 'max_lon')
    
    if minlat is None or minlon is None or maxlat is None or maxlon is None:
        return None
    
    minlat, minlon, maxlat, maxlon = map(float, (minlat, minlon, maxlat, maxlon))
    
    return Polygon([(minlon, minlat), (minlon, maxlat), (maxlon, maxlat), (maxlon, minlat), (minlon, minlat)])

def node_geometry(redis, node_key, ask_osm_api):
    """ Get a point geometry out of a node_key.
    """
    lat, lon = redis.hget(node_key, 'lat'), redis.hget(node_key, 'lon')
    
    if ask_osm_api and (lat is None or lon is None):
        xml = api_xml('http://api.openstreetmap.org/api/0.6/node/%s' % node_key[5:])
        node = xml.find('node')
        lat, lon = node.attrib['lat'], node.attrib['lon']
        remember_node(redis, node.attrib)
    
    if lat is None or lon is None:
        return None
    
    return Point(float(lon), float(lat))

def way_geometry(redis, way_key, ask_osm_api):
    """ Get a multipoint geometry for all the nodes of a way that we know.
    """
    way_nodes_key = way_key + '-nodes'
    
    node_ids = redis.lrange(way_nodes_key, 0, redis.llen(way_nodes_key))
    node_keys = ['node-' + node_id for node_id in node_ids]

    way_latlons = [(float(redis.hget(node_key, 'lat')), float(redis.hget(node_key, 'lon')))
                   for node_key in node_keys
                   if redis.exists(node_key)]
    
    # We only care about some of the nodes for a good-enough geometry
    needed = lambda things: len(things) / 3
    
    if ask_osm_api and (len(way_latlons) <= 1 or len(way_latlons) < needed(node_keys)):
        #
        # Too short, because we don't know enough. Ask OSM.
        #
        way_latlons = []
        
        try:
            xml = api_xml('http://api.openstreetmap.org/api/0.6/way/%s/full' % way_key[4:])
            nodes = xml.findall('node')
            
        except ExpatError:
            #
            # Parse can fail when a way has been deleted; check its previous version.
            #
            ver = int(redis.hget(way_key, 'version'))
            xml = api_xml('http://api.openstreetmap.org/api/0.6/way/%s/%d' % (way_key[4:], ver - 1))

            refs = [nd.attrib['ref'] for nd in xml.find('way').findall('nd')]
            nodes = []
            
            for offset in range(0, needed(refs), 10):
                xml = api_xml('http://api.openstreetmap.org/api/0.6/nodes?nodes=%s' % ','.join(refs[offset:offset+10]))
                nodes += xml.findall('node')
        
        for node in nodes:
            way_latlons.append((float(node.attrib['lat']), float(node.attrib['lon'])))
            remember_node(redis, node.attrib)

    if len(way_latlons) == 0:
        return None
    
    return MultiPoint([(lon, lat) for (lat, lon) in way_latlons])

def changeset_information(redis, changeset_key):
    """
    """
    user = redis.hget(changeset_key, 'user')
    created = redis.hget(changeset_key, 'created_at')
    
    if user is None or created is None:
        xml = api_xml('http://api.openstreetmap.org/api/0.6/changeset/%s' % changeset_key[10:])
        change = xml.find('changeset')
        
        if 'user' in change.attrib:
            remember_changeset(redis, change.attrib)
            user = redis.hget(changeset_key, 'user')
            created = redis.hget(changeset_key, 'created_at')
    
    return user, created, changeset_key[10:]

def overlaps(redis, area, changeset_key):
    """ Return true if an area and a changeset overlap.
    """
    even_close = area.buffer(5, 3) # 5 degrees of lat or lon is really far.
    changeset_items_key = changeset_key + '-items'
    
    object_keys = redis.smembers(changeset_items_key)
    node_keys = [key for key in object_keys if key.startswith('node-')]
    way_keys = [key for key in object_keys if key.startswith('way-')]
    rel_keys = [key for key in object_keys if key.startswith('relation-')]
    
    # check the node and ways twice, once ignoring the OSM API and once asking.
    
    for ask_osm_api in (False, True):
    
        change_bbox = changeset_bounds(redis, changeset_key, ask_osm_api)
        
        if change_bbox and change_bbox.disjoint(area):
            # definitely outside
            return False
        
        if change_bbox and change_bbox.within(area):
            # definitely inside
            return True

        for node_key in sorted(node_keys):
            node_geom = node_geometry(redis, node_key, ask_osm_api)
        
            if node_geom and node_geom.intersects(area):
                return True
            
            elif node_geom and not node_geom.within(even_close):
                # we're so far away that fuckit
                return False
        
        for way_key in sorted(way_keys):
            way_geom = way_geometry(redis, way_key, ask_osm_api)
            
            if way_geom and way_geom.intersects(area):
                return True
            
            elif way_geom and not way_geom.within(even_close):
                # we're so far away that fuckit
                return False
    
    # TODO: check the relations as well.
    
    for relation_key in sorted(rel_keys):
        relation_members_key = relation_key + '-members'
        #print change_key, relation_members_key, redis.smembers(relation_members_key)
    
    return False
