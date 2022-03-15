#!/usr/bin/env python
# coding: utf-8

# ## 1st TASK 

# In[1]:


import pandas as pd


# In[2]:


from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .appName("Spark Assignment")
        .getOrCreate())


# In[3]:


file = "C:/Users/maria/Downloads/Spark-Assignment/671009038_T_ONTIME_REPORTING.csv"


# In[4]:


flights = (spark.read.format("csv")
       .option("inferSchema", "true")
       .option("header", "true")
       .load(file))


# In[5]:


flights.show()


# In[6]:


flights.createOrReplaceTempView("flights")


# In[7]:


avg_dep_delay = spark.sql("""
SELECT avg(DEP_DELAY) AS Average_DEP_DELAY
FROM flights
""")
avg_dep_delay.show()


# In[8]:


avg_arr_delay = spark.sql("""
SELECT avg(ARR_DELAY) AS Average_ARR_DELAY
FROM flights
""")
avg_arr_delay.show()

