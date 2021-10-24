import petl as etl, psycopg2 as pg, sys, pymssql  as sql
import utm
import datetime
from psycopg2.extensions import adapt, register_adapter, AsIs



targetConn = pg.connect(dbname='dga2021', user='postgres', host='127.0.0.1', password='root') #grab value by referencing key dictionary

targetCursor = targetConn.cursor()

class Point(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y

def adapt_point(point):
    x = adapt(point.x)
    y = adapt(point.y)
    return AsIs("'(%s, %s)'" % (x, y))


register_adapter(Point, adapt_point)
#coord = Point(1.23, 4.56)
#coord_proc= adapt_point(coord)
#print(coord_proc)
targetCursor.execute("INSERT INTO point (point) VALUES (%s)",
      (Point(1.23, 4.56),))


targetConn.commit()

targetConn.close()


fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(fecha)
