# Databricks notebook source
from pyspark.testing import *
from pyspark.sql.functions import col,cast
from pyspark.sql.types import IntegerType

#define silver layer transform function
def total_electric_vehicles_byHour(df):
    df_withElectric = df.withColumn('electric_vehicles_count',df.EV_Car+df.EV_Bike)
    print("Successfully generated total ev vehicle count")
    return df_withElectric




# COMMAND ----------

# prepare test data
test_df = spark.read.format('csv').load('dbfs:/FileStore/tables/test_data.csv',header=True,inferSchema=True)

# filter useful columns
test_df = test_df.select(['EV Car','EV Bike']).toDF('EV_Car','EV_Bike')
test_df

# COMMAND ----------

# prepare expected data for assertion
expected_df = spark.createDataFrame(data = [(8,),(5,),(8,),(16,),(4,),(7,),(12,),(12,),(7,)],schema = ['electric_vehicles_count'])
expected_df = expected_df.select(expected_df.electric_vehicles_count.cast(IntegerType()))

# COMMAND ----------

# apply transform function over test_df
actual_df = total_electric_vehicles_byHour(test_df).select(['electric_vehicles_count'])

#test schema equality
assertSchemaEqual(actual_df.schema,expected_df.schema)
print("Schema test passed ..!")

# test dataframe equility
assertDataFrameEqual(actual_df,expected_df)
print("Dataframe equlity test passed ..!!")


