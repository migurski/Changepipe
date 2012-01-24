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

def node_geometry(redis, node_key):
    """ Get a point geometry out of a node_key.
    """
    lat = float(redis.hget(node_key, 'lat'))
    lon = float(redis.hget(node_key, 'lon'))

    return Point(lon, lat)

def way_geometry(redis, way_nodes_key):
    """ Get a multipoint geometry for all the nodes of a way that we know.
    """
    node_ids = redis.lrange(way_nodes_key, 0, redis.llen(way_nodes_key))
    node_keys = ['node-' + node_id for node_id in node_ids]

    way_latlons = [(float(redis.hget(node_key, 'lat')), float(redis.hget(node_key, 'lon')))
                   for node_key in node_keys
                   if redis.exists(node_key)]
    
    # We only care about some of the nodes for a good-enough geometry
    needed = lambda nodes: len(nodes) / 3
    
    if len(way_latlons) <= 1 or len(way_latlons) < needed(node_keys):
        #
        # Too short, because we don't know enough. Ask OSM.
        #
        way_latlons = []
        
        url = 'http://api.openstreetmap.org/api/0.6/way/%s/full' % way_nodes_key[4:-6]
        print >> stderr, url
        
        try:
            xml = parse(urlopen(url))
            nodes = xml.findall('node')
            
        except ExpatError:
            #
            # Parse can fail when a way has been deleted.
            #
            ver = int(redis.hget(way_nodes_key[:-6], 'version'))
            url = 'http://api.openstreetmap.org/api/0.6/way/%s/%d' % (way_nodes_key[4:-6], ver - 1)
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

    return MultiPoint([(lon, lat) for (lat, lon) in way_latlons])

def intersects(area, changeset_id):
    """
    """
    change_key = 'changeset-' + changeset_id
    
    for object_key in sorted(redis.smembers(change_key)):
        if object_key.startswith('node-'):
            node_key = object_key
            node_geom = node_geometry(redis, node_key)
            
            if node_geom.intersects(bbox):
                return True
        
        if object_key.startswith('way-'):
            way_nodes_key = object_key + '-nodes'
            way_geom = way_geometry(redis, way_nodes_key)
            
            if way_geom and way_geom.intersects(bbox):
                return True
        
        if object_key.startswith('relation-'):
            relation_members_key = object_key + '-members'
            #print change_key, relation_members_key, redis.smembers(relation_members_key)
    
    return False

germany = Polygon([(5.8, 47.3), (5.8, 55.0), (14.8, 55.0), (14.8, 47.3), (5.8, 47.3)])
usa = Polygon([(-125.0, 49.4), (-125.0, 24.7), (-66.8, 24.7), (-66.8, 49.4), (-125.0, 49.4)])

bbox = germany

for changeset_id in sorted(changesets):
    if intersects(bbox, changeset_id):
        print 'changeset/' + changeset_id
