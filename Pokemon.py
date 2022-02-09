#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
import psycopg2


# In[2]:


df2 = pd.read_csv("Pokemon.csv")
df2.head()


# In[3]:


df = pd.read_csv("types.csv")
df.head()


# In[4]:


df1= pd.read_csv("moves.csv")
df1.head()


# In[5]:


types_colum = df[['id','identifier']]
types_colum.head()


# In[6]:


types_colum = types_colum.rename(columns={"id": "TID","identifier": "Type"})

types_colum.head()


# In[7]:


types_colum.insert(0,"id",1)
types_colum['id'] = types_colum.index + 1
types_colum.head()


# In[8]:


types_colum.set_index("id",inplace=True)
types_colum['Type']=types_colum['Type'].str.capitalize()

types_colum.head()


# In[9]:


moves_colum = df1[['id','identifier','type_id']]
moves_colum.head()


# In[10]:


moves_colum = moves_colum.rename(columns={"id": "MID","identifier": "Moves","type_id": "TID"})

moves_colum.head()


# In[11]:


moves_colum.set_index(["MID"],inplace=True)

moves_colum.head()


# In[12]:


pokemon_colum = df2[["Name","Type 1","Attack","Defense","Speed"]]

pokemon_colum = pokemon_colum.rename(columns={"Name": "Pokemon_Name","Type 1": "Type"})

pokemon_colum.insert(0,"id",1)

pokemon_colum['id'] = pokemon_colum.index + 1

pokemon_colum.set_index("id", inplace=True)

pokemon_colum.head()


# In[13]:


pokemon_merged=pokemon_colum.merge(types_colum , on="Type" , how="outer")


# In[14]:


pokemon_merged_fix=pokemon_merged[['Pokemon_Name','TID','Type','Attack','Defense','Speed']].dropna()
pokemon_merged_fix.set_index("Pokemon_Name", inplace=True)
pokemon_merged_fix.head()


# In[15]:


connection_string = "<postgres>:<Sailakshmi@99>@localhost:5432/pokemon_db"
p_engine = create_engine(f'postgresql://postgres:Sailakshmi@localhost:5432/pokemon')


# In[16]:



df=pd.read_csv('Downloads/pokemon.csv',index_col=False)


# In[17]:


df = pd.DataFrame(types_colum)
df.to_csv('Downloads/types.csv')


# In[18]:


df = pd.DataFrame(moves_colum)
df.to_csv('Downloads/moves.csv')


# In[19]:


df = pd.DataFrame(pokemon_merged_fix)
df.to_csv('Downloads/pokemon.csv')


# In[27]:


df=pd.read_csv('Downloads/types.csv',index_col=False)
df.to_sql('types',p_engine,if_exists='replace',index=False)


# In[28]:


df=pd.read_csv('Downloads/moves.csv',index_col=False)
df.to_sql('moves',p_engine,if_exists='replace',index=False)


# In[29]:


df=pd.read_csv('Downloads/pokemon.csv',index_col=False)
df.to_sql('pokemon',p_engine,if_exists='replace',index=False)


# In[ ]:




