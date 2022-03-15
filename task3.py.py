#!/usr/bin/env python
# coding: utf-8

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


# ### 3rd TASK 

# In[7]:


flights = flights.filter("`ORIGIN` != 'AKN'")
flights = flights.filter("`ORIGIN` != 'PGV'")
flights = flights.filter("`ORIGIN` != 'DLG'")
flights = flights.filter("`ORIGIN` != 'GST'")
flights = flights.filter("`CARRIER` != 'HA'")

flights.show()


# In[8]:


from pyspark.sql.types import *
from pyspark.sql.functions import udf, expr, concat, col

flights_reduced = flights[['ORIGIN','CARRIER','DEP_TIME','DEP_DELAY']]


flights_reduced= flights_reduced.withColumn("DEP_TIME",flights_reduced['DEP_TIME'].cast(StringType()))
flights_reduced= flights_reduced.withColumn("DEP_TIME",expr("substring(DEP_TIME, 1, length(DEP_TIME)-2)"))
flights_reduced= flights_reduced.withColumn("DEP_TIME",flights_reduced['DEP_TIME'].cast(IntegerType()))
flights_reduced.cache()
flights_reduced.show()


# In[9]:


flights_reduced.limit(25)


# flights_reduced = flights_reduced.sample(False, fraction=0.05)

# (flights_reduced1, flights_reduced2) = flights_reduced.randomSplit([0.7, 0.3])

# In[10]:


prepped_dataframe = flights_reduced.na.fill(0)
prepped_dataframe.select("DEP_TIME").distinct().orderBy("DEP_TIME").show()


# In[11]:


from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCol= "DEP_TIME",outputCol= "DEP_TIME_vector")
flights_reduced = encoder.fit(flights_reduced)
flights_transformed = flights_reduced.transform(flights_reduced)


# In[12]:


from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

ORIGIN_indexer = StringIndexer().setInputCol("ORIGIN").setOutputCol("ORIGIN_index")
flights_reduced = ORIGIN_indexer.fit(flights_reduced).transform(flights_reduced)


# In[13]:


CARRIER_indexer = StringIndexer().setInputCol("CARRIER").setOutputCol("CARRIER_index")
flights_reduced = CARRIER_indexer.fit(flights_reduced).transform(flights_reduced)


# In[14]:


from pyspark.ml.feature import VectorAssembler

vector_assembler = VectorAssembler().setInputCols(["CARRIER_index", "ORIGIN_index", "DEP_TIME"]).setOutputCol("CARRIER_ORIGIN_TIME")


# transformation_pipeline = Pipeline()\
# .setStages([encoder, vector_assembler])
# fitted_pipeline = transformation_pipeline.fit(prepped_dataframe)
# 

# In[15]:


flights_reduced = vector_assembler.transform(flights_reduced)


# In[16]:


flights_reduced.show()


# In[ ]:


flights_reduced.dtypes


# In[ ]:


from pyspark.ml.regression import LinearRegression


# In[ ]:


splits = flights_reduced.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]


# In[ ]:


sc = spark.sparkContext

sc.setCheckpointDir('checkpoint')


# In[ ]:


lr=LinearRegression(featuresCol = 'CARRIER_ORIGIN_TIME', labelCol = 'DEP_DELAY',maxIter=10, 
                 regParam=0.3, elasticNetParam=0.8)

lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))


# so it seems that I have a problem with the command fit and the job cannot be completed probably due to memory issues which could not be resolved even after several tries. Following is the command predict that ne

# In[ ]:


lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","MV","features").show(10)


# In[ ]:


from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction",                  labelCol="MV",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))


# In[ ]:


test_result = lr_model.evaluate(test_df)

