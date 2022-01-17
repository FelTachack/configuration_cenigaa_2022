#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[27]:


import mysql.connector
from mysql.connector import errorcode
import subprocess
import re


# In[31]:


USER = 'jtachack_cenigaa'
PASS = 'TENNIS0360jft#'
DB = 'dbcenigaa'
TABLES = ['calificacion','dato_agroclimatico','datos','estacion_agroclimatica','metodos_obtencion','variable_agroclimatica']
COLUMNS_TABLES = {}
QUERY = {}
DATOS = {}
CONF = {}
DIRECTION = {}


# In[ ]:


subprocess.run(["python3","creacion_tablas.py"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
subprocess.call(["sh",'./importacion.sh'])


# In[32]:


for element in TABLES:
    QUERY['{}'.format(element)] = ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'".format(DB,element))


CONF['Creacion'] = ("ALTER TABLE datos ADD fecha_instalacion_dt DATETIME")
CONF['Actualizacion'] = ("UPDATE datos "
                        "SET fecha_instalacion_dt = STR_TO_DATE(FechaInstalacion,'%d/%m/%Y %H:%i')")
CONF['Depuracion'] = ("ALTER TABLE datos "
                     "DROP FechaInstalacion, RENAME COLUMN fecha_instalacion_dt TO FechaInstalacion")


# In[33]:


cnx = mysql.connector.connect(host = 'localhost', user= USER, password = PASS, database = DB)
cursor = cnx.cursor()


# In[34]:


def creacion_query(tabla_origen,tabla_destino,agrupador,columnas_origen,columnas_destino):
    
    query = "INSERT INTO {}".format(tabla_destino)
    query = query + "("
    for counter, n in enumerate(columnas_destino):
        if counter != len(columnas_destino)-1:
            query = query + "{},".format(COLUMNS_TABLES[tabla_destino][n])
        else:
            query = query + "{})".format(COLUMNS_TABLES[tabla_destino][n])
            
    query = query + "SELECT DISTINCT {}".format(agrupador)
    counter = 0
    for counter, n in enumerate(columnas_origen):
        query = query + ",{}".format(COLUMNS_TABLES[tabla_origen][n])
    
    query = query + " FROM {}".format(tabla_origen) + " GROUP BY {}".format(agrupador)
    
    counter = 0
    for counter, n in enumerate(columnas_origen):
            query = query + ",{}".format(COLUMNS_TABLES[tabla_origen][n])
    
    return query


# In[35]:


def creation_direction(tbl_dest,col_orig,col_dest,gr):
    tabla_origen = 'datos'
    DIRECTION ['{}'.format(tbl_dest)] = (tabla_origen,tbl_dest,col_orig,col_dest,gr)
    


# In[36]:


try:
    cursor.execute("USE {}".format(DB))
    cnx.database = DB
    print("Conexion establecida con {}: ".format(DB), end = '')
except mysql.connector.Error as err:
    print(err)
    exit(1)
else:
    print("OK")


# In[ ]:


for element in CONF:
    descripcion = CONF[element]
    try:
        print("{}:".format(element), end= '')
        cursor.execute(descripcion)
    except mysql.connector.Error as err:
        print(err)
        exit(1)
    else:
        print("OK")


# In[11]:


for query in QUERY:
    query_description = QUERY[query]
    print(query_description)
    try:
        cursor.execute(query_description)
        COLUMNS_TABLES['{}'.format(query)] = (cursor.fetchall())
    except mysql.connector.Error as err:
        print(err)


for table in COLUMNS_TABLES:
    list_one = list(COLUMNS_TABLES[table])
    list_one = list(map(lambda x: str(x),list_one))
    for counter, element in enumerate(list_one):
        list_one[counter] = re.sub("\(|\'|\'|\'|\,|\)","",element)
    
    COLUMNS_TABLES[table] = list_one


# In[12]:


#tbl_dest_estagro = 'estacion_agroclimatica'
#col_orig_estagro = [5,3,10,16,17,18,20]
#col_dest_estagro = [4,0,1,3,5,6,7,8]
#gr_estagro = 'Codigo_estacion'

#tbl_dest_varagro = 'variable_agroclimatica'
#col_orig_varagro = [19,]
#col_dest_varagro = [4,0,1,3,5,6,7,8]
#gr_varagro = 'Codigo_estacion'

#queyz = creation_direction(tbl_dest_datagro,col_orig_datagro,col_dest_datagro,gr_datagro)

#DATOS['{}'.format(tabla_destino)] = (creacion_query(tabla_origen,tabla_destino, agrupador,columnas_origen,columnas_destino))

