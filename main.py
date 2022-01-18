#!/usr/bin/env python
# coding: utf-8

# In[39]:


import subprocess


# In[37]:


# CREACION DE USUARIOS
#subprocess.run(['python3','administracion.py'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)


# In[38]:


# CREAR ESTRUCURA DE LA BASE DE DATOS
subprocess.run(['python3','creacion_tablas.py'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)


# In[ ]:


# CARGA DE DATOS NECESARIOS
subprocess.call(["sh",'./importacion.sh'])

# IMPORTACION DE DATOS
subprocess.run(['python3','importacion.py'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)


# In[15]:





# In[11]:





# In[21]:





# In[16]:





# In[17]:





# In[19]:





# In[22]:





# In[25]:





# In[26]:





# In[28]:





# In[29]:




