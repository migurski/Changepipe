from sys import stderr
from subprocess import Popen, PIPE
from xml.parsers.expat import ExpatError
from xml.etree.ElementTree import parse
from urllib import urlopen

from redis import StrictRedis
from shapely.geometry import Point, MultiPoint, Polygon

expiration = 3600

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

osmosis = 'osmosis --rri --simc --write-xml-change -'
osmosis = Popen(osmosis.split(), stdout=PIPE)

elements = changed_elements(osmosis.stdout)
osmosis.wait()

# make lists of Elements for node, ways, and relations
nodes = [node for node in elements if node.tag == 'node']
ways = [way for way in elements if way.tag == 'way']
relations = [rel for rel in elements if rel.tag == 'relation']

# keep a set of unique changesets to check on
changesets = set( [el.attrib['changeset'] for el in elements] )

# need: a list of changesets with their owners and extents that gets built up in redis
# redis wants: a geofenced channel with a list of changeset ids in it, potentially of limited length.
# changesets will build slowly over time, not using the API's bbox support because it's too blunt.
# redis structure for a changeset - ?
# ways: ask the API for their extent? keep them around with a timeout?

redis = StrictRedis()

for node in nodes:
    #
    # Save nodes to redis hashes under "node-id" keys, with keys "lat", "lon".
    # Also add node keys to associated changeset redis set.
    #
    pipe = redis.pipeline(True)
    node_key = 'node-%(id)s' % node.attrib
    change_key = 'changeset-%(changeset)s' % node.attrib
    
    remember_node(pipe, node.attrib)

    pipe.sadd(change_key, node_key)
    pipe.expire(change_key, expiration)
    pipe.execute()

for way in ways:
    #
    # Save ways to redis lists under "way-id-nodes" keys, with node id items.
    # Also add way keys to associated changeset redis set.
    #
    pipe = redis.pipeline(True)
    way_key = 'way-%(id)s' % way.attrib
    way_nodes_key = way_key + '-nodes'
    change_key = 'changeset-%(changeset)s' % way.attrib

    pipe.hset(way_key, 'version', way.attrib['version'])
    
    pipe.delete(way_nodes_key)
    for nd in way.findall('nd'):
        pipe.rpush(way_nodes_key, nd.attrib['ref'])
    
    pipe.sadd(change_key, way_key)
    pipe.expire(way_key, expiration)
    pipe.expire(way_nodes_key, expiration)
    pipe.expire(change_key, expiration)
    pipe.execute()

for relation in relations:
    #
    # Save relations to redis sets under "relation-id-members" keys, with 
    # node-id and way-id members. Also add relation keys to associated 
    # changeset redis set.
    #
    pipe = redis.pipeline(True)
    relation_key = 'relation-%(id)s' % relation.attrib
    relation_members_key = relation_key + '-members'
    change_key = 'changeset-%(changeset)s' % relation.attrib

    pipe.hset(relation_key, 'version', relation.attrib['version'])
    
    pipe.delete(relation_members_key)
    for member in relation.findall('member'):
        pipe.sadd(relation_members_key, '%(type)s-%(ref)s' % member.attrib)

    pipe.sadd(change_key, relation_key)
    pipe.expire(relation_key, expiration)
    pipe.expire(relation_members_key, expiration)
    pipe.expire(change_key, expiration)
    pipe.execute()

def node_geometry(redis, node_key, ask_osm_api):
    """ Get a point geometry out of a node_key.
    """
    lat, lon = redis.hget(node_key, 'lat'), redis.hget(node_key, 'lon')
    
    if ask_osm_api and (lat is None or lon is None):
        url = 'http://api.openstreetmap.org/api/0.6/node/%s' % node_key[5:]
        print >> stderr, url

        xml = parse(urlopen(url))
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
        
        url = 'http://api.openstreetmap.org/api/0.6/way/%s/full' % way_key[4:]
        print >> stderr, url
        
        try:
            xml = parse(urlopen(url))
            nodes = xml.findall('node')
            
        except ExpatError:
            #
            # Parse can fail when a way has been deleted; check its previous version.
            #
            ver = int(redis.hget(way_key, 'version'))
            url = 'http://api.openstreetmap.org/api/0.6/way/%s/%d' % (way_key[4:], ver - 1)
            print >> stderr, url

            xml = parse(urlopen(url))
            refs = [nd.attrib['ref'] for nd in xml.find('way').findall('nd')]
            nodes = []
            
            for offset in range(0, needed(refs), 10):
                url = 'http://api.openstreetmap.org/api/0.6/nodes?nodes=%s' % ','.join(refs[offset:offset+10])
                print >> stderr, url
    
                xml = parse(urlopen(url))
                nodes += xml.findall('node')
        
        for node in nodes:
            way_latlons.append((float(node.attrib['lat']), float(node.attrib['lon'])))
            remember_node(redis, node.attrib)

    if len(way_latlons) == 0:
        return None
    
    return MultiPoint([(lon, lat) for (lat, lon) in way_latlons])

def overlaps(area, changeset_id):
    """ Return true if an area and a changeset overlap.
    """
    even_close = area.buffer(5, 3) # 5 degrees of lat or lon is really far.
    change_key = 'changeset-' + changeset_id
    
    object_keys = redis.smembers(change_key)
    node_keys = [key for key in object_keys if key.startswith('node-')]
    way_keys = [key for key in object_keys if key.startswith('way-')]
    rel_keys = [key for key in object_keys if key.startswith('relation-')]
    
    # check the node and ways twice, once ignoring the OSM API and once asking.
    
    for ask_osm_api in (False, True):

        for node_key in sorted(node_keys):
            node_geom = node_geometry(redis, node_key, ask_osm_api)
        
            if node_geom and node_geom.intersects(area):
                return True
            
            elif node_geom and not node_geom.within(even_close):
                # we're so far away that fuckit
                print >> stderr, 'super-distant', node_key
                return False
        
        for way_key in sorted(way_keys):
            way_geom = way_geometry(redis, way_key, ask_osm_api)
            
            if way_geom and way_geom.intersects(area):
                return True
            
            elif way_geom and not way_geom.within(even_close):
                # we're so far away that fuckit
                print >> stderr, 'super-distant', way_key
                return False
    
    # TODO: check the relations as well.
    
    for relation_key in sorted(rel_keys):
        relation_members_key = relation_key + '-members'
        #print change_key, relation_members_key, redis.smembers(relation_members_key)
    
    return False

germany = Polygon([(5.8, 47.3), (5.8, 55.0), (14.8, 55.0), (14.8, 47.3), (5.8, 47.3)])
usa = Polygon([(-125.0, 49.4), (-125.0, 24.7), (-66.8, 24.7), (-66.8, 49.4), (-125.0, 49.4)])

bbox = germany

for changeset_id in sorted(changesets):
    if overlaps(bbox, changeset_id):
        print 'changeset/' + changeset_id
    else:
        print 'x', changeset_id
