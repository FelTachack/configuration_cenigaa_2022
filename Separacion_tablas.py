#!/usr/bin/env python
# coding: utf-8

# In[174]:


import mysql.connector
from mysql.connector import errorcode
import subprocess
import re


# In[195]:


USER = 'cenigaao_jftachack'
PASS = 'tennis0360JFT#'
DB = 'pruebadb'
TABLES = ['calificacion','dato_agroclimatico','datos','estacion_agroclimatica','metodos_obtencion','variable_agroclimatica']
COLUMNS_TABLES = {}
QUERY = {}
DATOS = {}


# In[196]:


for element in TABLES:
    QUERY['{}'.format(element)] = ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'".format(DB,element))


# In[197]:


cnx = mysql.connector.connect(host = 'localhost', user= USER, password = PASS, database = DB)
cursor = cnx.cursor()


# In[198]:


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





# In[199]:


for query in QUERY:
    query_description = QUERY[query]
    print(query_description)
    try:
        cursor.execute(query_description)
        COLUMNS_TABLES['{}'.format(query)] = (cursor.fetchall())
    except mysql.connector.Error as err:
        print(err)


# In[200]:



print(COLUMNS_TABLES)
for table in COLUMNS_TABLES:
    list_one = list(COLUMNS_TABLES[table])
    list_one = list(map(lambda x: str(x),list_one))
    for counter, element in enumerate(list_one):
        list_one[counter] = re.sub("\(|\'|\'|\'|\,|\)","",element)
    
    COLUMNS_TABLES[table] = list_one

print(COLUMNS_TABLES['estacion_agroclimatica'])
print(COLUMNS_TABLES['datos'][5])



# In[293]:


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
        if counter != len(columnas_origen)-1:
            query = query + ",{}".format(COLUMNS_TABLES[tabla_origen][n])
        else:
            query = query + " {}".format(COLUMNS_TABLES[tabla_origen][n])
    
    query = query + " FROM {}".format(tabla_origen) + " GROUP BY {}".format(agrupador)
    
    counter = 0
    for counter, n in enumerate(columnas_origen):
            query = query + ",{}".format(COLUMNS_TABLES[tabla_origen][n])
    
    return query


# In[295]:


columnas_destino = [4,0,1,3,5,6,7,8]
columnas_origen = [5,3,10,16,17,18,20]
tabla_origen = 'datos'
tabla_destino = 'estacion_agroclimatica'
agrupador = 'Codigo_estacion'
queryz = creacion_query(tabla_origen,tabla_destino, agrupador,columnas_origen,columnas_destino)


# In[296]:


print(queryz)
    


# In[260]:


len(datos_origen)

