# This is a sample Python script.
import petl as etl, psycopg2 as pg, sys, pymssql  as sql
import utm
import datetime
from psycopg2.extensions import adapt, register_adapter, AsIs
import os
# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
base_dir = os.path.dirname(os.path.realpath(__file__))

class Point(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y

def adapt_point(point):
    x = adapt(point.x)
    y = adapt(point.y)
    return AsIs("'(%s, %s)'" % (x, y))
try:
    #creo las conexiones
    sourceConn = sql.connect(#host=r'172.16.1.179\SATELITAL_PRO',
        host='172.16.1.119',
        user='c3l-lab',
        password='dga2021',
        database='DGA_BNA2000'
    )
    targetConn = pg.connect(dbname='dga2021', user='postgres', host='127.0.0.1', password='root') #grab value by referencing key dictionary

    sourceCursor = sourceConn.cursor()
    targetCursor = targetConn.cursor()

    sourceDs = etl.fromdb(sourceConn, "SELECT * FROM TG_PARAMETRO")
    etl.todb(sourceDs, targetConn, 'glo_parametros')

    # extrae y carga tipos de estaciones
    sourceDs = etl.fromdb(sourceConn, "SELECT * FROM TG_TIP_ESTACION_MAP")
    etl.todb(sourceDs, targetConn, 'glo_tipo_estacion')

    #elimino los datos de la tabla de destino
    targetCursor.execute("delete from ges_estaciones")

    #extraigo la data desde la fuente
    sourceCursor.execute("""select  ES.*, R.GLS_REGION, C.GLS_COMUNA from TG_ESTACION ES inner join TP_REGION_NUEVA R on R.cod_region = ES.cod_region
                            inner join TP_COMUNA C on C.cod_comuna = ES.cod_comuna""")
    sourceEstaciones = sourceCursor.fetchall()

    #recorro la data para transformar de utm a coordenadas
    for estacion in sourceEstaciones:
        coord = utm.to_latlon(estacion[16] ,estacion[17] ,
                              19,'K')

        LAT = float(coord[0])
        LONG = float(coord[1])
        #print("lat:" + str(LAT) + "long:" + str(LONG))
        FECHA_SUS = estacion[12]
        date_time_str = '1900-01-01 00:00:00.000'
        FECHA_ACTIVO = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
        date_time_obj_activo = FECHA_ACTIVO.date()
        date_time_obj = FECHA_SUS.date()
        estado_estacion = 0
        register_adapter(Point, adapt_point)

        if date_time_obj_activo == date_time_obj:
            estado_estacion = 1

        targetCursor.execute("INSERT INTO ges_estaciones  (cod_estacion, dig_estacion, gls_estacion,cod_institucio, cod_region " +
                            " ,cod_provincia, cod_comuna ,cod_topografia ,cod_lugar ,cod_reg_resp ,gls_direccion,fec_inicio ,fec_suspension "+
                            " ,gls_ubicacion,val_latitud,val_longitud,val_utm_este ,val_utm_norte,val_altitud,val_nivel_presion " +
                            " ,val_area_drena,cod_auxiliar,cod_ins_informa,gls_informante,fec_informante,flg_desfase, glosa_region,glosa_comuna, estado_estacion, coordinates ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           (estacion[0], estacion[1], estacion[2], estacion[3], estacion[4], estacion[5], estacion[6]
                           ,estacion[7], estacion[8], estacion[9], estacion[10], estacion[11], estacion[12], estacion[13]
                          ,LAT ,LONG , estacion[16], estacion[17], estacion[18], estacion[19], estacion[20], estacion[21]
                          ,estacion[22], estacion[23], estacion[24], estacion[25], estacion[26],estacion[27], estado_estacion, Point(LAT, LONG), ))


    targetConn.commit()

    targetCursor.execute("delete from ges_estacion_glosatipo")
    sourceCursor.execute(""" SELECT ab.COD_ESTACION , SUBSTRING(ab.TIPOS_EST,1, len(ab.TIPOS_EST) - 1) TIPO FROM ( SELECT COD_ESTACION ,
	        (SELECT
			STUFF(
			(SELECT GLS_TIPO_ESTAC   +' - '
			  FROM [DGA_BNA2000].[dbo].[TR_ESTA_TIP_ESTACION] R 
			INNER JOIN TG_TIP_ESTACION_MAP A  on R.TIP_ESTACION = A.TIP_ESTACION
			WHERE R.COD_ESTACION  = EST.COD_ESTACION
			FOR XML PATH (''))
			, 1, 0, '')  AS TIPOS_EST                    
              FROM [DGA_BNA2000].[dbo].[TR_ESTA_TIP_ESTACION] EST             
            INNER JOIN TG_TIP_ESTACION_MAP TIP  on est.TIP_ESTACION = tip.TIP_ESTACION
            where EST.COD_ESTACION = p.COD_ESTACION
            GROUP BY COD_ESTACION) AS TIPOS_EST 
            FROM [DGA_BNA2000].[dbo].[TR_ESTA_TIP_ESTACION] P            
            INNER JOIN TG_TIP_ESTACION_MAP J  on P.TIP_ESTACION = J.TIP_ESTACION
            GROUP BY P.COD_ESTACION) AS ab""")

    sourceTipoGlosa = sourceCursor.fetchall()

    # recorro la data para transformar de utm a coordenadas
    for glosa in sourceTipoGlosa:

        targetCursor.execute(
            "INSERT INTO ges_estacion_glosatipo  (cod_estacion, glosa_tipo) " +
            "  VALUES(%s, %s)",
            (glosa[0], glosa[1],))

    targetConn.commit()


    #extrae y carga estaciones con su tipo
    sourceDs = etl.fromdb(sourceConn, "SELECT * FROM TR_ESTA_TIP_ESTACION")
    etl.todb(sourceDs, targetConn, 'ges_estacion_tipo')

    sourceDs = etl.fromdb(sourceConn, "SELECT  * FROM TD_ESTAD_OFICIAL WHERE YEAR(FEC_MEDICION) >= 2020")
    etl.todb(sourceDs, targetConn, 'TD_ESTAD_OFICIAL')

    sourceCursor.close()
    targetConn.close()

except pg.Error as e:
    print(sys.exc_info()[0], "occurred.")
    print(e.pgerror)
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    f = open(base_dir + "\logError.txt" , "a")
    f.write(str(sys.exc_info()[0]) + "occurred." + str(e.pgerror) + fecha)
    f.close()

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
