from subprocess import Popen, PIPE

from redis import StrictRedis
from shapely.geometry import Polygon

from Changepipe import osm

osmosis = 'osmosis --rri --simc --write-xml-change -'
osmosis = Popen(osmosis.split(), stdout=PIPE)

elements = osm.changed_elements(osmosis.stdout)
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
    
    osm.remember_node(pipe, node.attrib)

    pipe.sadd(change_key, node_key)
    pipe.expire(change_key, osm.expiration)
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
    pipe.expire(way_key, osm.expiration)
    pipe.expire(way_nodes_key, osm.expiration)
    pipe.expire(change_key, osm.expiration)
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
    pipe.expire(relation_key, osm.expiration)
    pipe.expire(relation_members_key, osm.expiration)
    pipe.expire(change_key, osm.expiration)
    pipe.execute()

germany = Polygon([(5.8, 47.3), (5.8, 55.0), (14.8, 55.0), (14.8, 47.3), (5.8, 47.3)])
usa = Polygon([(-125.0, 49.4), (-125.0, 24.7), (-66.8, 24.7), (-66.8, 49.4), (-125.0, 49.4)])

bbox = germany

for changeset_id in sorted(changesets):
    if osm.overlaps(redis, bbox, changeset_id):
        print 'changeset/' + changeset_id
    else:
        print 'x', changeset_id
