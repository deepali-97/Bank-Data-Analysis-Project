import pyspark
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import col

sc = SparkContext("local","Project 1")
sqlContext = SQLContext(sc)

#1. Load data and create Spark data frame 
bankDF = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("escape","\"").option("delimiter",";").load("D:\Dataset\Bank.txt")
bankDF.printSchema()
bankDF.show()

#2. A Give marketing success rate. (No. of people subscribed / total no. of entries)
#B Give marketing failure rate
total_entries = bankDF.count()
total_subscribers = bankDF.filter(bankDF.y.contains("yes")).count()
success_rate = total_subscribers / total_entries
y = "{:.2f}".format(success_rate)
failure_rate = (1 - success_rate)
x = "{:.2f}".format(failure_rate)
print("Marketing Success Rate : ",y)
print("Marketing Failure Rate : ",x)

#3. Maximum, Mean, and Minimum age of average targeted customer
df_age = bankDF.filter(bankDF.y.contains("yes")).agg(F.min(bankDF.age).alias("min_age") , F.max(bankDF.age).alias("max_age"), F.avg(bankDF.age).alias("avg_age") )
df_age.select(df_age.min_age, df_age.max_age, (F.round(df_age["avg_age"], 2).alias("avg_age"))).show()
    
#4. Check quality of customers by checking average balance, median balance of customers 
bankDF.agg(F.avg(bankDF.balance).alias("avg_balance"),F.expr('percentile(balance, 0.5)').alias("median_balance")).show()

#5. Check if age status mattered for subscription to deposit.
age_df = bankDF.filter(bankDF.y.contains("yes")).groupBy("age").count()
age_df.show()
def age_comp(age):
     str1 = ""
     age_str = str(age)
     if age < 20 :
             str1 =  "Subscribers are mostly of " + age_str +  " years and are teenagers"
     elif age >=20 and age  <=50 :
             str1 =  "Subscribers are mostly of " + age_str +" years and are adults"
     else:
             str1 =  "Subscribers are mostly of " + age_str + " years and are elders"
     return str1
age_df.registerTempTable("age_count")
max_count_age = sqlContext.sql("select age,count from age_count where count= (select max(count) from age_count)")
convertAgeUDF = udf(lambda z : age_comp(z))
print("Age matters for subscription to deposit")
max_count_age.withColumn("age",col("age").cast("Integer")).select( convertAgeUDF(col("age")).alias("----STATUS----")).show(truncate = False)

#6.Check if marital status mattered for subscription to deposit.
marital_df = bankDF.filter(bankDF.y.contains("yes")).groupBy("marital").count()
marital_df.show()
def marital_comp(marital):
     str = ""
     if marital == "married" :
             str =  "Subscribers are mostly married"
     elif marital == "single":
             str = "Subscribers are mostly single"
     else:
             str = "Subscribers are mostly divorced"
     return str
marital_df.registerTempTable("marital_count")
max_count = sqlContext.sql("select marital,count from marital_count where count= (select max(count) from marital_count)")
convertUDF = udf(lambda z : marital_comp(z))
print("Marital Status matters for subscription to deposit")
max_count.select( convertUDF(max_count.marital).alias("----STATUS----")).show(truncate = False)

#7.Check if age and marital status together mattered for subscription to deposit scheme
age_marital_df = bankDF.filter(bankDF.y.contains("yes")).groupBy("age","marital").count()
age_marital_df.show()
def age_marital_comp(age,marital):
     str1 = "Subscribers are mostly of " + age + " years of age and are " + marital 
     return str1
age_marital_df.registerTempTable("age_marital_count")
max_a_m = sqlContext.sql("select age,marital,count from age_marital_count where count= (select max(count) from age_marital_count)")
convertAgeMaritalUDF = udf(lambda y,z : age_marital_comp(y,z))
print("Both Age and Marital Status matters for subscription to deposit") 
max_a_m.select(convertAgeMaritalUDF(col("age"),col("marital")).alias("----STATUS----")).show(truncate = False)



