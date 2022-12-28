#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[ ]:





# In[9]:


spark = SparkSession.builder.appName('nppes').config("spark.jars", "mysql-connector-j-8.0.31.jar").getOrCreate()


# In[10]:


spark.version


# In[13]:


df = spark.read.csv('C:\\Other-Programs\\NPPES\\ORIGINAL_NPPES_Data_Dissemination_December_2022\\npidata_pfile_20050523-20221211.csv',
                    header=True, inferSchema=True)


# In[14]:


df


# In[15]:


df.show()


# In[ ]:





# In[16]:


df.printSchema()


# In[18]:


df = df .withColumnRenamed("Provider Business Practice Location Address Country Code (If outside U.S.)","Provider Business Practice Location Address Country Code") .withColumnRenamed("Provider Business Practice Location Address Country Code (If outside U.S.)","Provider Business Practice Location Address Country Code")
df.printSchema()


# In[ ]:





# In[ ]:





# In[ ]:


df.write   .mode("overwrite")   .format("jdbc")   .option("driver","com.mysql.jdbc.Driver")   .option("url", "jdbc:mysql://localhost:3306/nppes")   .option("dbtable", "npidata_pfile_20050523_20221211")   .option("user", "admin")   .option("password", "admin")   .save()


# In[ ]:





# In[ ]:




