from subprocess import Popen, PIPE
from xml.parsers.expat import ParserCreate
from xml.etree.ElementTree import parse
from urllib import urlopen

osmosis = 'osmosis --rri --simc --write-xml-change -'.split()
osmosis = Popen(osmosis, stdout=PIPE)

parser = ParserCreate()
nodes, ways, changesets, unknowns = set(), set(), set(), set()

def start_element(name, attrs):
    if name == 'node' and 'id' in attrs:
        nodes.add(attrs['id'])
    
    if name == 'way' and 'id' in attrs:
        ways.add(attrs['id'])
    
    if name == 'nd' and attrs['ref'] not in nodes:
        unknowns.add(attrs['ref'])
    
    if 'changeset' in attrs:
        changesets.add(attrs['changeset'])

parser.StartElementHandler = start_element
parser.ParseFile(osmosis.stdout)

osmosis.wait()

print len(nodes), 'nodes'
print len(ways), 'ways'
print len(unknowns), 'unknowns'
print len(changesets), 'changesets:', ', '.join(changesets)

for changeset in changesets:

    changeset = urlopen('http://api.openstreetmap.org/api/0.6/changeset/%s' % changeset)
    changeset = parse(changeset).find('changeset').attrib
    
    print changeset['id'], changeset['user'], changeset['open']
