from subprocess import Popen, PIPE
from xml.parsers.expat import ParserCreate
from xml.etree.ElementTree import parse
from urllib import urlopen

from redis import StrictRedis
from shapely.geometry import Point, MultiPoint

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

    pipe.hset(node_key, 'version', node.attrib['version'])
    pipe.hset(node_key, 'lat', node.attrib['lat'])
    pipe.hset(node_key, 'lon', node.attrib['lon'])
    
    pipe.sadd(change_key, node_key)
    pipe.expire(node_key, expiration)
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
    
    if len(way_latlons) <= 1:
        return None

    return MultiPoint([(lon, lat) for (lat, lon) in way_latlons])

for changeset in sorted(changesets):
    
    change_key = 'changeset-' + changeset
    
    for object_key in sorted(redis.smembers(change_key)):
        if object_key.startswith('node-'):
            node_key = object_key
            print change_key, node_key, node_geometry(redis, node_key)
        
        if object_key.startswith('way-'):
            way_nodes_key = object_key + '-nodes'
            print change_key, way_nodes_key, way_geometry(redis, way_nodes_key)
        
        if object_key.startswith('relation-'):
            relation_members_key = object_key + '-members'
            print change_key, relation_members_key, redis.smembers(relation_members_key)
            
