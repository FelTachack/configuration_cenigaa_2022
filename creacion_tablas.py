#!/usr/bin/env python
# coding: utf-8

# In[8]:


from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode
import subprocess


# In[9]:


DB_NAME = 'pruebadb'
RUTA_DHIME = '/home/tachack/Documents/Entrega3/dhime_table.sql'
PASS = 'tennis0360JFT#'


# In[16]:


TABLES = {}
TABLES['estacion_agroclimatica'] = (
  "CREATE TABLE estacion_agroclimatica("
    "idstation INT NOT NULL  primary key,"
    "nombre VARCHAR(50),"
    "descripcion VARCHAR(80),"
    "latitud FLOAT,"
    "longitud FLOAT,"
    "departamento VARCHAR(40),"
    "municipio VARCHAR(40),"
    "elevacion INT,"
    "fecha_instalacion DATE"
")")


TABLES["variable_agroclimatica"] = (
    "CREATE TABLE variable_agroclimatica("
    "idvariable INT NOT NULL primary key,"
    "identificador VARCHAR(40),"
    "parametro VARCHAR(20),"
    "idstation INT NOT NULL,"
    "unidad VARCHAR(10),"
    "etiqueta VARCHAR(30),"
    "fecha_creacion DATE,"
    "foreign key (idstation) REFERENCES estacion_agroclimatica(idstation)"
")")

TABLES["calificacion"] = (
    "CREATE TABLE calificacion("
    "identificador INT NOT NULL primary key,"
    "descripcion VARCHAR(40),"
    "color VARCHAR(20)"
")")

TABLES["metodos_obtencion"] = (
    "CREATE TABLE metodos_obtencion("
    "identificador INT NOT NULL primary key,"
    "etiqueta VARCHAR(40),"
    "descripcion VARCHAR(20)"
")")


TABLES["dato_agroclimatico"] = (
    "CREATE TABLE dato_agroclimatico("
    "idvariable INT NOT NULL,"
    "valor_numerico FLOAT,"
    "aprobacion INT NOT NULL,"
    "metodo_obtencion INT,"
    "timestamp DATETIME,"
    "calificacion VARCHAR(10),"
    "foreign key (idvariable) REFERENCES variable_agroclimatica(idvariable),"
    "foreign key (aprobacion) REFERENCES calificacion(identificador),"
    "foreign key (metodo_obtencion) REFERENCES metodos_obtencion(identificador)"
")")



# In[17]:


cnx = mysql.connector.connect(host = 'localhost', user = 'cenigaao_jftachack', password = PASS)
cursor = cnx.cursor()


# In[18]:


def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


# In[19]:


try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)


# In[20]:


for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()


# In[4]:





# In[14]:





# In[5]:





# In[ ]:





# In[24]:





# In[25]:





# In[26]:





# In[31]:





# In[30]:




