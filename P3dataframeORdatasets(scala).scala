// Databricks notebook source
val dataset = Seq(1, 2, 3).toDS()
dataset.show()

// COMMAND ----------

case class Person(name: String, age:Int)
val personDS = Seq(Person("Max",33),Person("Adam",32),Person("Muller",62)).toDS()
personDS.show()

// COMMAND ----------

val personDS1 = Seq(("Max",33),("Adam",32),("Muller",62)).toDS()
personDS1.show()

// COMMAND ----------

// DBTITLE 1,Create a Dataset from an RDD
val rdd = sc.parallelize(Seq((1,"Spark"),(2,"Databricks")))
val integerDS = rdd.toDS()
integerDS.show()

// COMMAND ----------

// DBTITLE 1,Create a Dataset from a DataFrame
//You can call df.as[SomeCaseClass] to convert the DataFrame to a Dataset.

case class Company(name:String,fountingYear:Int, numEmployees:Int)
val inputSeq = Seq(Company("ABC",1998,310),Company("XYZ",1983,904),Company("NOP",2005,83))
val df = sc.parallelize(inputSeq).toDF()
val companyDS = df.as[Company]
companyDS.show()

// COMMAND ----------

val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
val df = rdd.toDF("Id", "Name")

val dataset = df.as[(Int, String)]
dataset.show()

// COMMAND ----------

val wordsDataset = sc.parallelize(Seq("Spark I am your mother", "May the spark be with you", "Spark I am your mother")).toDS()
val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
                                 .filter(_ != "")
                                 .groupBy("value")
val countsDataset = groupedDataset.count()
countsDataset.show()

// COMMAND ----------

val path = "dbfs:/FileStore/shared_uploads/sari27012000te@gmail.com/newex.txt"
val df1 = spark.read.text(path)
df1.show()

// COMMAND ----------

 val rddFromFile = spark.sparkContext.textFile(path)

// COMMAND ----------

val rddFromFile = spark.sparkContext.textFile(path)
rddFromFile.foreach(f=>{
    println(f)
  })

// COMMAND ----------

rddFromFile.count()

// COMMAND ----------

rddFromFile.first()

// COMMAND ----------



val lineswithword = rddFromFile.filter(line=>line.contains("examples"))
lineswithword.count()

// COMMAND ----------

lineswithword.collect().foreach(f=>{println(f)})

// COMMAND ----------

val dataset = rddFromFile.toDS()

// COMMAND ----------

val groupedDataset = dataset.flatMap(_.toLowerCase.split(" "))
                                 .filter(_ != "")
                                 .groupBy("value")
val countsDataset = groupedDataset.count()
countsDataset.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

val dataset = rddFromFile.toDS()
val result = dataset
              .flatMap(_.split(" "))               // Split on whitespace
              .filter(_ != "")                     // Filter empty words
              .map(_.toLowerCase())
              .toDF()                              // Convert to DataFrame to perform aggregation / sorting
              .groupBy($"value")                   // Count number of occurrences of each word
              .agg(count("*") as "numOccurances")
              .orderBy($"numOccurances" desc)      // Show most common words first
result.show()

// COMMAND ----------


