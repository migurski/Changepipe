from subprocess import Popen, PIPE

from redis import StrictRedis
from shapely.geometry import Polygon
from shapely import wkt

from Changepipe import osm

places = [line.split('\t', 1) for line in open('places.txt', 'r')]
places = [(name, wkt.loads(geom)) for (name, geom) in places]

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

for changeset_id in sorted(changesets):
    #
    # Delete saved changeset bounding boxes, because they've probably changed.
    #
    changeset_key = 'changeset-' + changeset_id

    redis.hdel(changeset_key, 'min_lat')
    redis.hdel(changeset_key, 'min_lon')
    redis.hdel(changeset_key, 'max_lat')
    redis.hdel(changeset_key, 'max_lon')

for node in nodes:
    #
    # Save nodes to redis hashes under "node-id" keys, with keys "lat", "lon".
    # Also add node keys to associated changeset redis set.
    #
    pipe = redis.pipeline(True)
    node_key = 'node-%(id)s' % node.attrib
    change_items_key = 'changeset-%(changeset)s-items' % node.attrib
    
    osm.remember_node(pipe, node.attrib)

    pipe.sadd(change_items_key, node_key)
    pipe.expire(change_items_key, osm.expiration)
    pipe.execute()

for way in ways:
    #
    # Save ways to redis lists under "way-id-nodes" keys, with node id items.
    # Also add way keys to associated changeset redis set.
    #
    pipe = redis.pipeline(True)
    way_key = 'way-%(id)s' % way.attrib
    way_nodes_key = way_key + '-nodes'
    change_items_key = 'changeset-%(changeset)s-items' % way.attrib

    pipe.hset(way_key, 'version', way.attrib['version'])
    
    pipe.delete(way_nodes_key)
    for nd in way.findall('nd'):
        pipe.rpush(way_nodes_key, nd.attrib['ref'])
    
    pipe.sadd(change_items_key, way_key)
    pipe.expire(way_key, osm.expiration)
    pipe.expire(way_nodes_key, osm.expiration)
    pipe.expire(change_items_key, osm.expiration)
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
    change_items_key = 'changeset-%(changeset)s-items' % relation.attrib

    pipe.hset(relation_key, 'version', relation.attrib['version'])
    
    pipe.delete(relation_members_key)
    for member in relation.findall('member'):
        pipe.sadd(relation_members_key, '%(type)s-%(ref)s' % member.attrib)

    pipe.sadd(change_items_key, relation_key)
    pipe.expire(relation_key, osm.expiration)
    pipe.expire(relation_members_key, osm.expiration)
    pipe.expire(change_items_key, osm.expiration)
    pipe.execute()

for changeset_id in sorted(changesets):
    changeset_key = 'changeset-' + changeset_id
    
    for (name, geom) in places:
        if osm.overlaps(redis, geom, changeset_key):
            print 'changeset/' + changeset_id, 'by', redis.hget(changeset_key, 'user'), 'in', name
