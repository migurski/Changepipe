from sys import argv
from shapely import wkt

if __name__ == '__main__':
    for (index, line) in enumerate(open(argv[1])):
        name, geom = line.split('\t', 1)
        geom = wkt.loads(geom)
        print 'line', index + 1, '-', name, '%.3f' % geom.area
