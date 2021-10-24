import pymssql
import pandas as pd
import os

conn = pymssql.connect(host = r'BUVMSQLWIND01\BACO',
                    user = 'usuarioveine',
                    password = 'usuarioveine',
                    database='VE_INE')
cursor = conn.cursor()

SQL_Query = pd.read_sql_query(
'''SELECT top 1000 nombre, direccion, edad,sexo FROM pruebaPandas''', conn)

df = pd.DataFrame(SQL_Query, columns=['nombre','direccion','edad','sexo'])


base_dir = os.path.dirname(os.path.realpath(__file__))

compression_opts = dict(method='zip',
                        archive_name='out.csv')
df.to_csv(base_dir +  '\out.zip', index=False,
          compression=compression_opts)