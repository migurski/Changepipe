from sys import argv
from shapely import wkt
from re import split

if __name__ == '__main__':
    for (index, line) in enumerate(open(argv[1])):
        name, geom = split(r'\s+', line, 1)
        geom = wkt.loads(geom)
        print 'line', index + 1, '-', name, '%.3f' % geom.area,
        xmin, ymin, xmax, ymax = geom.bounds
        print '(%(ymax).3f, %(xmin).3f, %(ymin).3f, %(xmax).3f)' % locals()
