from subprocess import Popen, PIPE
from xml.parsers.expat import ParserCreate
from xml.etree.ElementTree import parse
from urllib import urlopen

from redis import StrictRedis

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

nodes = [node for node in elements if node.tag == 'node']
ways = [way for way in elements if way.tag == 'way']
relations = [rel for rel in elements if rel.tag == 'relation']

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
    change_key = 'changeset-%(changeset)s' % node.attrib

    pipe.delete(way_nodes_key)
    for nd in way.findall('nd'):
        pipe.rpush(way_nodes_key, nd.attrib['ref'])
    
    pipe.sadd(change_key, way_key)
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

    pipe.delete(relation_members_key)
    for member in relation.findall('member'):
        pipe.sadd(relation_members_key, '%(type)s-%(ref)s' % member.attrib)

    pipe.sadd(change_key, relation_key)
    pipe.expire(relation_members_key, expiration)
    pipe.expire(change_key, expiration)
    pipe.execute()
