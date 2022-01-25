#!/usr/bin/env python
# coding: utf-8

# In[8]:


# CODIGO PARA LA ESTRUCURACION DE LA BASE DE DATOS


# In[41]:


import mysql.connector 
from mysql.connector import errorcode
import re


# In[42]:


config = {
  'user': 'jtachack_remoto',
  'password': 'TENNIS0360jft#',
  'host': '162.248.54.159',
  'raise_on_warnings': True
}


# In[43]:


DB_NAME = 'pruebadb'


# In[44]:


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
    "fecha_instalacion DATETIME"
")")


TABLES["variable_agroclimatica"] = (
    "CREATE TABLE variable_agroclimatica("
    "idvariable VARCHAR(100) NOT NULL primary key,"
    "identificador VARCHAR(80),"
    "parametro VARCHAR(20),"
    "idstation INT NOT NULL,"
    "unidad VARCHAR(10),"
    "etiqueta VARCHAR(30),"
    "fecha_creacion DATETIME,"
    "foreign key (idstation) REFERENCES estacion_agroclimatica(idstation)"
")")

TABLES["Aprobacion"] = (
    "CREATE TABLE Aprobacion("
    "identificador INT primary key,"
    "descripcion VARCHAR(40),"
    "color VARCHAR(20)"
")")

TABLES["metodos_obtencion"] = (
    "CREATE TABLE metodos_obtencion("
    "identificador VARCHAR(40) primary key,"
    "etiqueta VARCHAR(40),"
    "descripcion VARCHAR(20)"
")")


TABLES["dato_agroclimatico"] = (
    "CREATE TABLE dato_agroclimatico("
    "idvariable VARCHAR(100) NOT NULL,"
    "valor_numerico FLOAT,"
    "aprobacion INT,"
    "metodo_obtencion VARCHAR(40),"
    "timestamp DATETIME,"
    "calificacion VARCHAR(10),"
    "foreign key (idvariable) REFERENCES variable_agroclimatica(idvariable),"
    "foreign key (aprobacion) REFERENCES Aprobacion(identificador),"
    "foreign key (metodo_obtencion) REFERENCES metodos_obtencion(identificador)"
")")


# In[45]:


print("Probando conexión:",end = '') # TESTEO DE CONEXION
try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
except mysql.connector.Error as err:
    print(err.msg)
else:
    print("OK")


# In[46]:


def create_database(cursor): # FUNCION PARA CREAR LA BASE DE DATOS
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


# In[47]:


try: # INSTRUCCION PARA CREAR BASE DE DATOS
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


# In[48]:



for table_name in TABLES: # INSTRUCCION PARA CREAR TABLAS
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

cnx.close()
        


# In[ ]:





# In[ ]:




