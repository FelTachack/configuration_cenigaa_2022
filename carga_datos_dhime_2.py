#!/usr/bin/env python
# coding: utf-8

# In[1]:


#CODIGO PARA LA IMPORTACION DE DATOS


# In[49]:


import mysql.connector
from mysql.connector import errorcode
import re


# In[50]:


config = {
  'user': 'jtachack_remoto',
  'password': 'TENNIS0360jft#',
  'host': '162.248.54.159',
  'raise_on_warnings': True
}


# In[51]:


DB_NAME = 'pruebadb'
TABLES = ['Aprobacion','dato_agroclimatico','datos','estacion_agroclimatica','metodos_obtencion','variable_agroclimatica']
COLUMNS_TABLES = {}
COLUMNS_NAMES = {}
DATOS = {}
CONF = {}
DIRECTION = {}


# In[52]:


#Definicion de funcion para la query de insercion de datos
def creacion_query(tabla_destino,agrupador,columnas_origen,columnas_destino):
    tabla_origen = 'datos'
    
    query = "INSERT INTO {}".format(tabla_destino)
    query = query + "("
    for counter, n in enumerate(columnas_destino):
        if counter != len(columnas_destino)-1:
            query = query + "{},".format(COLUMNS_TABLES[tabla_destino][n])
        else:
            query = query + "{})".format(COLUMNS_TABLES[tabla_destino][n])
            
    query = query + "SELECT {}".format(agrupador)
    counter = 0
    for counter, n in enumerate(columnas_origen):
        query = query + ",{}".format(COLUMNS_TABLES[tabla_origen][n])
    
    query = query + " FROM {}".format(tabla_origen) + " GROUP BY {}".format(agrupador)
    
    counter = 0
    for counter, n in enumerate(columnas_origen):
            query = query + ",{}".format(COLUMNS_TABLES[tabla_origen][n])
    
    return query


# In[53]:


def creation_direction(tbl_dest,col_orig,col_dest,gr):
    tabla_origen = 'datos'
    DIRECTION ['{}'.format(tbl_dest)] = (tabla_origen,tbl_dest,col_orig,col_dest,gr)


# In[54]:


# Captura de informacion de las tablas
for element in TABLES:
    COLUMNS_NAMES['{}'.format(element)] = ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'".format(DB_NAME,element))


# In[55]:


# Conexion
print("Probando conexión y uso de base datos '{}': ".format(DB_NAME),end = '') # TESTEO DE CONEXION
try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print(err.msg)
else:
    print("OK")


# In[56]:


# Definición del acondicionamiento de los datos

CONF['Tarea_1'] = ("ALTER TABLE datos ADD fecha_instalacion_dt DATETIME")
CONF['Tarea_2']=  ("ALTER TABLE datos ADD COLUMN id_variable VARCHAR(100)")
CONF['Tarea_3'] = ("UPDATE datos "                        
                   "SET fecha_instalacion_dt = STR_TO_DATE(FechaInstalacion,'%d/%m/%Y %H:%i')")
CONF['Tarea_4'] = ("UPDATE datos SET id_variable = CONCAT(Codigo_estacion,Etiqueta)")
CONF['Tarea_5'] = ("ALTER TABLE datos "
                     "DROP FechaInstalacion, RENAME COLUMN fecha_instalacion_dt TO FechaInstalacion")

CONF['Tarea_6'] = ("ALTER TABLE datos ADD fecha_dt DATETIME")
CONF['Tarea_7'] = ("UPDATE datos "
                   "SET fecha_dt = STR_TO_DATE(Fecha,'%Y-%m-%d %H:%i')")
CONF['Tarea_8'] = ("ALTER TABLE datos "
                   "DROP Fecha, RENAME COLUMN fecha_dt TO Fecha")
#CONF['Tarea_8'] = ("INSERT INTO variable_agroclimatica(id_variable,Etiqueta,identificador,id_station,parametro) SELECT DISTINCT id_variable, Etiqueta, Descripcionserie,Codigo_estacion,idParametro FROM datos GROUP BY id_variable, Etiqueta,Descripcionserie,Codigo_estacion,idParametro")



# In[57]:


# Ejecución de acondicionamiento
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


# In[58]:


cnx.close()


# In[59]:


#Obtencion de listado de columnnas de cada tabla

for query in COLUMNS_NAMES:
    query_description = COLUMNS_NAMES[query]
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


# In[60]:


cnx.close()
try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print(err.msg)
else:
    print("OK")


# In[61]:


#Importacion de datos

tbl_dest_estagro = 'estacion_agroclimatica'
gr_estagro = 'Codigo_estacion'
col_orig_estagro = [5,3,10,17,18,19,21]
col_dest_estagro = [4,0,1,3,5,6,7,8]


tbl_dest_varagro = 'variable_agroclimatica'
col_orig_varagro = [19,]
col_dest_varagro = [4,0,1,3,5,6,7,8]
gr_varagro = 'Codigo_estacion'

DATOS['{}'.format(tbl_dest_estagro)] = (creacion_query(tbl_dest_estagro,gr_estagro,col_orig_estagro,col_dest_estagro))

for data_instruction in DATOS:
    query_data_instruction = DATOS[data_instruction]
    print("copiando informacion")
    try:
        cursor.execute(query_data_instruction)
    except mysql.connector.Error as err:
        print(err)
    else:
        print("OK")
    


# In[9]:


DATOS['{}'.format('variable_agroclimatica')] = ('INSERT INTO variable_agroclimatica(idvariable,etiqueta,identificador,idstation,parametro) SELECT DISTINCT id_variable,Etiqueta,Descripcionserie,Codigo_estacion,idParametro FROM datos')
DATOS['{}'.format('Aprobacion')] = ('INSERT INTO Aprobacion(identificador) SELECT DISTINCT NivelAprobacion FROM datos')
DATOS['{}'.format('metodos_obtencion')] = ('INSERT INTO metodos_obtencion(identificador) SELECT DISTINCT Calificador FROM datos')


# In[59]:



DATOS['{}'.format('consulta')] = ('SELECT dato_agroclimatico.valor_numerico,variable_agroclimatica.idvariable, variable_agroclimatica.parametro,estacion_agroclimatica.nombre FROM dato_agroclimatico INNER JOIN variable_agroclimatica ON variable_agroclimatica.idvariable = dato_agroclimatico.idvariable INNER JOIN estacion_agroclimatica ON estacion_agroclimatica.idstation = variable_agroclimatica.idstation')


# In[48]:





# In[10]:


cnx.close()

