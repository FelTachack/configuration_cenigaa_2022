#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


df = pd.read_csv(r'/home/tachack/Documents/Entrega1/nrma_stations24JUL2021.csv', encoding = "ISO-8859-1",delimiter=',')
df_2 = pd.read_csv(r'/home/tachack/Documents/Entrega1/station_id.csv', encoding = "UTF8", delimiter=',',header= None)


# In[21]:


latitudes = [2.783,2.783,1.93048,1.72236,3.217,1.80283,1.85431,1.93048,1.883,2.883]
longitudes = [-75.267,-75.267,-76.2164,-76.1318,-75.217,-75.8902, -76.0516, -76.2164,-76.267,-75.433]
municipios = ['Rivera','Rivera','Isnos','Palestina','Villavieja','Acevedo','Pitalito','Isnos','San Agustin','Palermo']
estaciones = {}
departamento = 'Huila'
descripcion = 'agro-meteorológica'
fecha_instalacion_1 = '2021-08-21 00:00'


lista_estaciones = df_2.head().values.tolist()

lista_estaciones.append(['Modulo de transmisión - Acevedo',8006])
lista_estaciones.append(['Modulo de transmisión - Pitalito',8007])
lista_estaciones.append(['Modulo de transmisión - Isnos',8008])
lista_estaciones.append(['Modulo de transmisión - San Agustin',8009])
lista_estaciones.append(['Modulo de transmisión - Reserva natural la tribuna',8010])

for count,element in enumerate(lista_estaciones):
    
    estaciones['{}'.format(element[1])] = ([['{}'.format(element[1]),'{}'.format(element[0]),descripcion,
                                           latitudes[count],longitudes[count],departamento,municipios[count],'',fecha_instalacion_1]])


# In[22]:


header = ['idstation','nombre','descripcion','latitud','longitud','departamento','municipio','elevacion','fecha_instalacion']


# In[23]:


df_3 = pd.DataFrame(columns=header)
for element in estaciones:
    df_3 = pd.concat([pd.DataFrame(estaciones[element], columns=df_3.columns), df_3], ignore_index=True)


# In[25]:


df_3


# In[168]:


df_3.to_csv(path_or_buf='~/Desktop/estaciones.csv',index=False)


# In[7]:


fecha_creacion = '2021-08-21 00:00'
parametros = ['TEMPERATURA','TEMP SUELO','VELVIENTO','DIRVIENTO','HUM RELATIVA','PRECIPITACION','HUM SUELO',
             'RAD SOLAR','PAR']


# In[5]:


identificadores = ['Temperatura del aire',
                   'Temperatura del suelo sensor 1','Temperatura del suelo sensor 2','Temperatura del suelo sensor 3','Temperatura del suelo sensor 4',
                  'Direccion del viento en grados','velocidad del viento','Humedad relativa','Precipitacion total',
                  'Humedad del suelo sensor 1', 'Humedad del suelo sensor 2','Humedad del suelo sensor 3','Humedad del suelo sensor 4',
                  'Radiacion solar Global','Radiacion par']


# In[224]:


estaciones_2 = []
for counter,element in enumerate(lista_estaciones):
    estaciones_2.append(lista_estaciones[counter][1])


# In[6]:


etiquetas = ['TA2_AUT_1','TS10_AUT_1_1','TS10_AUT_1_2','TS10_AUT_1_3','TS10_AUT_1_4','VVAG_CON_1','DV_AUT_1','HRA10_AUT_1','PINTPG_CON_1',
            'HRS10_AUT_1_1','HRS10_AUT_1_2','HRS10_AUT_1_3','HRS10_AUT_1_4','RSA_AUT_1','RPA_AUT_1']


# In[8]:


id_variable = []
unidad = ''
variables_agroclimaticas = {}
for element in estaciones:
    for counter,etiqueta in enumerate(etiquetas):
        identificador = identificadores[counter]
        idstation = element
        idvariable = str(element) + etiqueta
        if counter < 1:
            parametro = parametros[0]
            
        elif counter >= 1 and counter < 5:
            parametro = parametros[1]
        
        elif counter >=5 and counter< 6:
            parametro = parametros[2]
        elif counter >=6 and counter< 7:
            parametro = parametros[3]
        elif counter >=6 and counter< 7:
            parametro = parametros[4]
        elif counter >=7 and counter< 8:
            parametro = parametros[4]
        elif counter >=8 and counter< 9:
            parametro = parametros[5]
        elif counter >=9 and counter< 13:
            parametro = parametros[6]
        elif counter >=13 and counter< 14:
            parametro = parametros[7]
        elif counter >=14 and counter< 15:
            parametro = parametros[8]
        id_variable.append(idvariable)    
        variables_agroclimaticas['{}'.format(idvariable)] = ([[idvariable,identificador,parametro,idstation,unidad,etiqueta,fecha_creacion]])
        
   
 


# In[242]:


header_2 = ['idvariable','identificador','parametro','idstation','unidad','etiqueta','fecha_creacion']
df_4 = pd.DataFrame(columns=header_2)
for element in variables_agroclimaticas:
    df_4 = pd.concat([pd.DataFrame(variables_agroclimaticas[element], columns=df_4.columns), df_4], ignore_index=True)
   


# #### estaciones

# In[245]:


df_4.to_csv(path_or_buf='~/Desktop/variables.csv',index=False)


# In[9]:


# Dato_agroclimatico

aprobacion_d = 900
calificacion_d = 30
metodo_obtencion_d = "REGISTRADOR"
header_3 = ['id_variable','valor_numerico','aprobacion','metodo_obtencion','timestamp','calificacion']


# In[17]:


df_var  = df[["id_station","par_radiation","time_saved"]]
indexNames = df_var[ df_var['id_station'] == 0 ].index
df_var.drop(indexNames , inplace=True)
indexNames = df_var[ df_var['id_station'] == 9 ].index
df_var.drop(indexNames , inplace=True)
indexNames = df_var[ df_var['id_station'] == 6 ].index
df_var.drop(indexNames , inplace=True)
#dato_agroclimatico['{}'.format('')] = ([[id_variable,valor_numerico,aprobacion,metodo_obtencion,timestamp,calificacion]])


# In[353]:


etiquetas


# In[18]:


df_var.assign(aprobacion = aprobacion_d)
df_var['aprobacion'] = '{}'.format(aprobacion_d)

df_var.assign(metodo_obtencion = metodo_obtencion_d)
df_var['metodo_obtencion'] = '{}'.format(metodo_obtencion_d)

df_var.assign(calificacion = calificacion_d)
df_var['calificacion'] = '{}'.format(calificacion_d)

df_var["id_variable"] = df_var["id_station"].astype(str) + 'RPA_AUT_1'
#df_var['id_variable'] = df_var.apply(lambda x : [i + 1 for i, e in enumerate(x['WD'])], axis=1)


# In[19]:


#df_var_2 = df_var[["id_variable","temperature","aprobacion","metodo_obtencion","time_saved","calificacion"]]
#df_var_2.to_csv(path_or_buf='~/Desktop/dato_temperatura.csv',index=False, header=None)

df_var_3 = df_var[["id_variable","par_radiation","aprobacion","metodo_obtencion","time_saved","calificacion"]]
df_var_3.to_csv(path_or_buf='~/Desktop/dato_par_radiation.csv',index=False, header=None)


# In[20]:


df_var_3


# In[18]:


df


# In[332]:


df_var


# In[321]:



set(df_var_2["id_variable"].tolist())


# In[334]:


SELECT
  TABLE_NAME AS `Tabla`,
  ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) AS `Tamaño (MB)`
FROM
  information_schema.TABLES
WHERE
  TABLE_SCHEMA = "pruebadb"
ORDER BY
  (DATA_LENGTH + INDEX_LENGTH)
DESC


# In[ ]:


SELECT table_name, table_rows 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'pruebadb';

