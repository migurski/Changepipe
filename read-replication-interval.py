from sys import argv
from subprocess import Popen, PIPE
from operator import itemgetter
from StringIO import StringIO
from urllib import quote

from redis import StrictRedis
from boto.s3.connection import S3Connection
from shapely.geometry import Polygon
from shapely import wkt

from Changepipe import osm

# load places file
places = [line.split('\t', 1) for line in open('places.txt', 'r')]
places = [(name, wkt.loads(geom)) for (name, geom) in places]

# get Amazon S3 details
access, secret, bucket = argv[1:]
bucket = S3Connection(access, secret).get_bucket(bucket)

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

keys = dict()

for (name, geom) in places:
    keys[name] = bucket.new_key('%s.xml' % name)

for changeset_id in sorted(changesets):
    changeset_key = 'changeset-' + changeset_id
    
    for (name, geom) in places:
        place_changesets_key = 'place-' + name + '-changesets'
    
        try:
            if osm.overlaps(redis, geom, changeset_key):
                user, created, changeset_id = osm.changeset_information(redis, changeset_key)
                print 'changeset/' + changeset_id, 'by', user, 'in', name, 'at', created
                
                redis.sadd(place_changesets_key, changeset_key)
            
            else:
                print '  not', changeset_id
        
        except Exception, e:
            print 'Exception (A):', e

for (name, geom) in places:
    place_changesets_key = 'place-' + name + '-changesets'
    
    changesets = []
    
    for changeset_key in redis.smembers(place_changesets_key):
        try:
            user, created, changeset_id = osm.changeset_information(redis, changeset_key)
        except Exception, e:
            print 'Exception (B):', e
        else:
            changesets.append((user, created, changeset_id))
    
    changesets.sort(key=itemgetter(1), reverse=True)
    changesets = changesets[:25]
    
    pipe = redis.pipeline(True)
    pipe.delete(place_changesets_key)
    
    for (user, created, changeset_id) in changesets:
        pipe.sadd(place_changesets_key, 'changeset-' + changeset_id)
    
    pipe.execute()

    feed = StringIO()
    
    print >> feed, '<?xml version="1.0" encoding="utf-8"?>'
    print >> feed, '<feed xmlns="http://www.w3.org/2005/Atom">'
    print >> feed, '<title type="text">%(name)s</title>' % locals()
    
    for (user, created, changeset_id) in changesets:
        user_href = 'http://www.openstreetmap.org/user/%s' % quote(user)
        change_href = 'http://www.openstreetmap.org/browse/changeset/%s' % quote(changeset_id)
        
        print >> feed, '<entry>'
        print >> feed, '<title>Changeset %(changeset_id)s</title>' % locals()
        print >> feed, '<updated>%(created)s</updated>' % locals()
        print >> feed, '<content type="html"><a href="%(user_href)s">%(user)s</a> edited OpenStreetMap in <a href="%(change_href)s">changeset %(changeset_id)s</a>.</content>' % locals()
        print >> feed, '<author><name>%(user)s</name><uri>%(user_href)s</uri></author>' % locals()
        print >> feed, '<link href="%(change_href)s"/>' % locals()
        print >> feed, '<id>%(change_href)s</id>' % locals()
        print >> feed, '</entry>'

    print >> feed, '</feed>'
    
    listing = bucket.new_key(name + '.xml')
    listing.set_contents_from_string(feed.getvalue(), headers={'Content-Type': 'application/atom+xml'}, policy='public-read')
    
    print 'http://%s.s3.amazonaws.com/%s' % (bucket.name, listing.name)
