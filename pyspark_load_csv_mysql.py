#!/usr/bin/env python
# coding: utf-8

# ## initial setup

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when


# In[2]:


spark = SparkSession.builder.appName('nppes').config("spark.jars", "mysql-connector-j-8.0.31.jar").getOrCreate()


# In[3]:


df = spark.read.csv('C:\\Other-Programs\\NPPES\\ORIGINAL_NPPES_Data_Dissemination_December_2022\\npidata_pfile_20050523-20221211.csv',
                    header=True, inferSchema=True)


# In[4]:


df


# In[5]:


df.show()


# In[6]:


df.printSchema()


# In[7]:


df = df .withColumnRenamed("Employer Identification Number (EIN)", "EIN") .withColumnRenamed("Provider Last Name (Legal Name)", "Provider Last Name") .withColumnRenamed("Provider Business Mailing Address Country Code (If outside U.S.)","Provider Business Mailing Address Country Code") .withColumnRenamed("Provider Business Practice Location Address Country Code (If outside U.S.)","Provider Business Practice Location Address Country Code")

df.printSchema()


# In[8]:


df = df.select([col(c).alias(c.replace(' ', '_')) for c in df.columns])

df.printSchema()


# In[ ]:





# ## formatting data

# In[9]:


df2 = df.alias('df2')


# In[16]:


df2.count()


# In[11]:


df2.show()


# In[25]:


df2 = df2.where(
    col("Provider_License_Number_1").isNotNull() |
    col("Provider_License_Number_2").isNotNull() |
    col("Provider_License_Number_3").isNotNull() |
    col("Provider_License_Number_4").isNotNull() |
    col("Provider_License_Number_5").isNotNull() |
    col("Provider_License_Number_6").isNotNull() |
    col("Provider_License_Number_7").isNotNull() |
    col("Provider_License_Number_8").isNotNull() |
    col("Provider_License_Number_9").isNotNull() |
    col("Provider_License_Number_10").isNotNull() |
    col("Provider_License_Number_11").isNotNull() |
    col("Provider_License_Number_12").isNotNull() |
    col("Provider_License_Number_13").isNotNull() |
    col("Provider_License_Number_14").isNotNull() |
    col("Provider_License_Number_15").isNotNull() |
    col("Other_Provider_Identifier_Issuer_1").isNotNull() |
    col("Other_Provider_Identifier_Issuer_2").isNotNull() |
    col("Other_Provider_Identifier_Issuer_3").isNotNull() |
    col("Other_Provider_Identifier_Issuer_4").isNotNull() |
    col("Other_Provider_Identifier_Issuer_5").isNotNull() |
    col("Other_Provider_Identifier_Issuer_6").isNotNull() |
    col("Other_Provider_Identifier_Issuer_7").isNotNull() |
    col("Other_Provider_Identifier_Issuer_8").isNotNull() |
    col("Other_Provider_Identifier_Issuer_9").isNotNull() |
    col("Other_Provider_Identifier_Issuer_10").isNotNull() |
    col("Other_Provider_Identifier_Issuer_11").isNotNull() |
    col("Other_Provider_Identifier_Issuer_12").isNotNull() |
    col("Other_Provider_Identifier_Issuer_13").isNotNull() |
    col("Other_Provider_Identifier_Issuer_14").isNotNull() |
    col("Other_Provider_Identifier_Issuer_15").isNotNull() |
    col("Other_Provider_Identifier_Issuer_16").isNotNull() |
    col("Other_Provider_Identifier_Issuer_17").isNotNull() |
    col("Other_Provider_Identifier_Issuer_18").isNotNull() |
    col("Other_Provider_Identifier_Issuer_19").isNotNull() |  
    col("Other_Provider_Identifier_Issuer_20").isNotNull() |
    col("Other_Provider_Identifier_Issuer_21").isNotNull() |
    col("Other_Provider_Identifier_Issuer_22").isNotNull() |
    col("Other_Provider_Identifier_Issuer_23").isNotNull() |
    col("Other_Provider_Identifier_Issuer_24").isNotNull() |
    col("Other_Provider_Identifier_Issuer_25").isNotNull() |
    col("Other_Provider_Identifier_Issuer_26").isNotNull() |
    col("Other_Provider_Identifier_Issuer_27").isNotNull() |
    col("Other_Provider_Identifier_Issuer_28").isNotNull() |
    col("Other_Provider_Identifier_Issuer_29").isNotNull() |   
    col("Other_Provider_Identifier_Issuer_30").isNotNull() |
    col("Other_Provider_Identifier_Issuer_31").isNotNull() |
    col("Other_Provider_Identifier_Issuer_32").isNotNull() |
    col("Other_Provider_Identifier_Issuer_33").isNotNull() |
    col("Other_Provider_Identifier_Issuer_34").isNotNull() |
    col("Other_Provider_Identifier_Issuer_35").isNotNull() |
    col("Other_Provider_Identifier_Issuer_36").isNotNull() |
    col("Other_Provider_Identifier_Issuer_37").isNotNull() |
    col("Other_Provider_Identifier_Issuer_38").isNotNull() |
    col("Other_Provider_Identifier_Issuer_39").isNotNull() | 
    col("Other_Provider_Identifier_Issuer_40").isNotNull() |
    col("Other_Provider_Identifier_Issuer_41").isNotNull() |
    col("Other_Provider_Identifier_Issuer_42").isNotNull() |
    col("Other_Provider_Identifier_Issuer_43").isNotNull() |
    col("Other_Provider_Identifier_Issuer_44").isNotNull() |
    col("Other_Provider_Identifier_Issuer_45").isNotNull() |
    col("Other_Provider_Identifier_Issuer_46").isNotNull() |
    col("Other_Provider_Identifier_Issuer_47").isNotNull() |
    col("Other_Provider_Identifier_Issuer_48").isNotNull() |
    col("Other_Provider_Identifier_Issuer_49").isNotNull() | 
    col("Other_Provider_Identifier_Issuer_50").isNotNull()
         )


# In[26]:


df2.count()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# ## dump to db

# In[ ]:


df.write   .mode("overwrite")   .format("jdbc")   .option("driver","com.mysql.jdbc.Driver")   .option("url", "jdbc:mysql://localhost:3306/nppes")   .option("dbtable", "npidata_pfile_20050523_20221211_2")   .option("user", "admin")   .option("password", "admin")   .save()


# In[ ]:





# In[ ]:




