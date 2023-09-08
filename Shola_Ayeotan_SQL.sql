-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ##### Loading the clinical trial data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC # Loading the CSV file as an RDD
-- MAGIC clinicaltrialRDD = sc.textFile("/FileStore/tables/clinicaltrial_2021.csv")
-- MAGIC
-- MAGIC # Saving the first row as the header
-- MAGIC header = clinicaltrialRDD.first()
-- MAGIC
-- MAGIC # Removing the header row and splitting the remaining rows by "|" delimiter
-- MAGIC clinicaltrialRDD = clinicaltrialRDD.filter(lambda row: row != header)\
-- MAGIC                           .map(lambda row: row.split("|"))
-- MAGIC
-- MAGIC # Define schema for clinical trial data
-- MAGIC clinicaltrialSchema = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Submission",StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True)])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Converting the RDD to a DataFrame with the specified schema
-- MAGIC clinicaltrialDF = spark.createDataFrame(clinicaltrialRDD, clinicaltrialSchema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Printing the schema
-- MAGIC clinicaltrialDF.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Showing the first 10 rows of the clinical trial data
-- MAGIC clinicaltrialDF.show(10)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Loading the pharma data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharmaDF = spark.read.csv("/FileStore/tables/pharma.csv", header=True, inferSchema=True)
-- MAGIC     
-- MAGIC pharmaDF.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Creating a temporary view.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Creating temporary view for clinical trial data
-- MAGIC clinicaltrialDF.createOrReplaceTempView("clinicaltrialSQL")
-- MAGIC
-- MAGIC # Creating temporary view for pharma data
-- MAGIC pharmaDF.createOrReplaceTempView("pharmaSQL")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Question 1

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) as number_of_distinct_studies
FROM clinicaltrialSQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Question 2

-- COMMAND ----------

SELECT Type, COUNT(*) AS frequency
FROM clinicaltrialSQL
GROUP BY Type
ORDER BY frequency DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC
-- MAGIC type_pd = spark.sql("SELECT Type, COUNT(*) AS frequency \
-- MAGIC                 FROM clinicaltrialSQL \
-- MAGIC                 GROUP BY Type \
-- MAGIC                 ORDER BY frequency DESC") \
-- MAGIC                 .toPandas()
-- MAGIC
-- MAGIC plt.figure(figsize=(8,4))
-- MAGIC sns.set_style("whitegrid")
-- MAGIC sns.barplot(x='frequency', y='Type', data=type_pd)
-- MAGIC plt.xlabel('Frequency', fontsize=12)
-- MAGIC plt.ylabel('Type', fontsize=12)
-- MAGIC plt.title('Distribution of Clinical Trial Types', fontsize=16)
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Question 3

-- COMMAND ----------

SELECT condition, COUNT(*) as frequency
FROM 
(SELECT EXPLODE(SPLIT(Conditions, ",")) AS condition
FROM clinicaltrialSQL
WHERE Conditions != '')
GROUP BY condition
ORDER BY frequency DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Question 4

-- COMMAND ----------

SELECT Sponsor, COUNT(*) as frequency
    FROM clinicaltrialSQL
    WHERE Sponsor NOT IN 
        (SELECT DISTINCT Parent_Company
        FROM pharmaSQL)
    GROUP BY Sponsor
    ORDER BY frequency DESC
    LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Question 5

-- COMMAND ----------

SELECT Completion AS Completion_Date, count(*) AS Completed_Trials
FROM clinicaltrialSQL
WHERE year(to_date(Completion, 'MMM yyyy')) = 2021 AND Status = 'Completed'
GROUP BY Completion_Date
ORDER BY to_date(Completion_Date, 'MMM yyyy')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Creating a variable for the result
-- MAGIC MonthlyCompletions = spark.sql("SELECT Completion AS Completion_Date, count(*) AS Completed_Trials \
-- MAGIC                                 FROM clinicaltrialSQL \
-- MAGIC                                 WHERE year(to_date(Completion, 'MMM yyyy')) = 2021 AND Status = 'Completed' \
-- MAGIC                                 GROUP BY Completion_Date \
-- MAGIC                                 ORDER BY to_date(Completion_Date, 'MMM yyyy')") 
-- MAGIC
-- MAGIC
-- MAGIC # Converting it to pandas dataframe
-- MAGIC MonthlyCompletions = MonthlyCompletions.toPandas()
-- MAGIC
-- MAGIC
-- MAGIC # Creating a line plot
-- MAGIC fig, ax = plt.subplots(figsize=(12,6))
-- MAGIC ax.plot(MonthlyCompletions['Completion_Date'], MonthlyCompletions['Completed_Trials'])
-- MAGIC ax.set_xlabel('Completion Date')
-- MAGIC ax.set_ylabel('Completed Trials')
-- MAGIC ax.set_title('Distribution of Completed Trials in 2021')
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Further Analysis 3 (Spark SQL)
-- MAGIC
-- MAGIC Count the number of clinical trials sponsored by each pharmaceutical company listed in the dataset (based on the Sponsor column).

-- COMMAND ----------

SELECT Sponsor, COUNT(*) as frequency
FROM clinicaltrialSQL
WHERE Sponsor IN 
(SELECT DISTINCT Parent_Company
FROM pharmaSQL)
GROUP BY Sponsor
ORDER BY frequency DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import seaborn as sns
-- MAGIC
-- MAGIC # Creating a variable for the result
-- MAGIC PharmaSponsorships = spark.sql("SELECT Sponsor, COUNT(*) as frequency \
-- MAGIC                           FROM clinicaltrialSQL \
-- MAGIC                           WHERE Sponsor IN (SELECT DISTINCT Parent_Company FROM pharmaSQL) \
-- MAGIC                           GROUP BY Sponsor \
-- MAGIC                           ORDER BY frequency DESC")
-- MAGIC
-- MAGIC # Converting it to pandas dataframe
-- MAGIC PharmaSponsorships = PharmaSponsorships.toPandas()
-- MAGIC
-- MAGIC # Plotting a bar chart using seaborn
-- MAGIC sns.set(style="whitegrid")
-- MAGIC sns.barplot(x="frequency", y="Sponsor", data=PharmaSponsorships.head(10))
-- MAGIC plt.xlabel("Frequency")
-- MAGIC plt.ylabel("Sponsor")
-- MAGIC plt.title("Top 10 Sponsors with Most Clinical Trials")
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Extra Feature 
-- MAGIC
-- MAGIC Question 3: Count the number of clinical trials sponsored by each pharmaceutical company listed in the dataset (based on the Sponsor column).

-- COMMAND ----------

SELECT Parent_Company, SUM(CAST(REGEXP_REPLACE(Penalty_Amount, '[$,]', '') AS FLOAT)) As HighestPenaltiesPaid 
FROM pharmaSQL 
GROUP BY Parent_Company, Penalty_Amount 
ORDER BY HighestPenaltiesPaid DESC LIMIT 10
