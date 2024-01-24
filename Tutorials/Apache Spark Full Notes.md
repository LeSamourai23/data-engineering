# Chapter 1: Introduction
## What is Apache Spark ?
Apache Spark is a unified computing engine and a set of libraries for parallel data processing on computer clusters.
A group of machines alone is not powerful, you need a framework  
to coordinate work across them. Spark does just that, managing and coordinating the  execution of tasks on data across a cluster of computers.

![[Pasted image 20240124214034.png]]

# Chapter 2: A Gentle Introduction to Spark
## What is a cluster ?
A cluster, or group, of computers, pools the resources of many machines  
together, giving us the ability to use all the cumulative resources as if they were a single computer.

## Spark Applications
Spark Applications consist of a driver process and a set of executor processes. The **driver** process runs your main() function, sits on a node in the cluster, and is responsible for three things: *maintaining information about the Spark Application*; *responding to a user’s program or input*; and *analyzing, distributing, and scheduling work across the executors (discussed momentarily)*. The driver process is absolutely essential—it’s the heart of a Spark Application and maintains all relevant information during the lifetime of the application.  
The **executors** are responsible for actually carrying out the work that the driver  
assigns them. This means that each executor is responsible for only two things: executing code assigned to it by the driver, and reporting the state of the computation on that executor back to the driver node.

![[Pasted image 20240124214710.png]]

**Summary:**
• Spark employs a cluster manager that keeps track of the resources available.  
• The driver process is responsible for executing the driver program’s commands across the executors to complete a given task

## Spark APIs
Spark has two fundamental sets of APIs: the low-level “unstructured” APIs, and the higher-level structured APIs. We discuss both in this book, but these introductory chapters will focus primarily on the higher-level structured APIs.

## SparkSession
The SparkSession instance is the way Spark executes user-defined manipulations across the cluster. There is a one-tone correspondence between a SparkSession and a Spark Application.

In Python you’ll see something like this:  
```python
<pyspark.sql.session.SparkSession at 0x7efda4c1ccd0>
```

A simple task of creating a range of numbers:
```python
myRange = spark.range(1000).toDF("number")
```

## DataFrames
A DataFrame is the most common Structured API and simply represents a table of data with rows and columns. The list that defines the columns and the types within those columns is called the schema.

![[Pasted image 20240124215457.png]]

## Partitions
To allow every executor to perform work in parallel, Spark breaks up the data into chunks called **partitions**. A partition is a collection of rows that sit on one physical machine in your cluster. A DataFrame’s partitions represent how the data is physically distributed across the cluster of machines during execution.

If you have one partition, Spark will have a parallelism of only one, even if you have thousands of executors. If you have many partitions but only one executor, Spark will still have a parallelism of only one because there is only one computation resource.

## Transformations
A simple transformation to find all even numbers in a DataFrame:
```python
divisBy2 = myRange.where("number % 2 = 0")
```

There are two types of transformations: those that specify **narrow dependencies**, and those that specify **wide dependencies**.

Transformations consisting of **narrow dependencies** (we’ll call them *narrow transformations*) are those for which each input partition will contribute to only one output partition. In the preceding code snippet, the where statement specifies a narrow dependency.

![[Pasted image 20240124220135.png]]

A **wide dependency** (or *wide transformation*) style transformation will have input partitions contributing to many output partitions.
You will often hear this referred to as a **shuffle** whereby Spark will exchange partitions across the cluster. With narrow transformations, Spark will automatically perform an operation called **pipelining**, meaning that if we specify multiple filters on DataFrames, they’ll all be performed in-memory. The same cannot be said for shuffles. When we perform a shuffle, Spark writes the results to disk.

![[Pasted image 20240124220705.png]]

## Lazy Evaluation
Lazy evaluation in Spark means it delays executing operations on data until the last moment. Instead of immediately changing data, Spark plans transformations for efficiency. It compiles this plan just before executing, optimizing the entire data flow. For instance, Spark uses **predicate pushdown** to automatically optimize tasks like fetching a single record efficiently in large jobs.

## Actions
Transformations allow us to build up our logical transformation plan. To trigger the computation, we run an action. An action instructs Spark to compute a result from a series of transformations.
Example of a simple action is *count*, which gives us the total num‐  
ber of records in the DataFrame:
```python
divisBy2.count()
```

## An End-to-End Example

Defining our DataFrame:
```python
flightData2010 = spark.read.option("inferSchema", "true").option("header", "true").csv("/FileStore/shared_uploads/ayaan2911@aol.com/summary2010.csv")
```
![[Pasted image 20240124231513.png]]

the `take` function is an action that retrieves the first N elements of a distributed collection:
```python
flightData2010.take(3)
```
![[Pasted image 20240124231643.png]]

![[Pasted image 20240124231722.png]]

We can call `explain` on any DataFrame object to see the DataFrame’s lineage (or how Spark will execute this query):
```python
flightData2010.sort("count").explain()
```
![[Pasted image 20240124231837.png]]

By default, when we perform a shuffle, Spark outputs 200 shuffle partitions. Let’s set this value to 5 to reduce the number of the output partitions from the shuffle:
```python
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2010.sort("count").take(2)
```
![[Pasted image 20240124232037.png]]

#### SparkSQL
You can make any DataFrame into a table or view with one simple method call:
```python
flightData2010.createOrReplaceTempView("flight_data_2010")
```

Let's do some simple transformationn using sparkSQL and compare it to the DataFrame way:
```python
sqlWay = spark.sql("""
                   SELECT DEST_COUNTRY_NAME, count(1) FROM flight_data_2010
                   GROUP BY DEST_COUNTRY_NAME
                   """)

dataFrameWay = flightData2010.groupby("DEST_COUNTRY_NAME").count()

sqlWay.explain()
dataFrameWay.explain()
```
![[Pasted image 20240124232342.png]]
Notice that these plans compile to the exact same underlying plan.

We will use the max function, to establish the maximum number of flights to and from any given location. This just scans each  value in the relevant column in the DataFrame and checks whether it’s greater than the previous values that have been seen.
```python
spark.sql("SELECT max(count) FROM flight_data_2010").take(1)

from pyspark.sql.functions import max
flightData2010.select(max("count")).take(1)
```
![[Pasted image 20240124232530.png]]

Finding the top five destination countries in the data:
```python
maxSql = spark.sql("""
                   SELECT DEST_COUNTRY_NAME, sum(count) as destination_total FROM flight_data_2010
                   GROUP BY DEST_COUNTRY_NAME
                   ORDER BY sum(count) DESC
                   LIMIT 5""")

maxSql.show()
```
![[Pasted image 20240124232649.png]]

Let’s move to the DataFrame syntax that is semantically similar but slightly different in implementation and ordering:
```python
from pyspark.sql.functions import desc

flightData2010.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "destination_total").sort(desc("destination_total")).limit(5).show()
```
![[Pasted image 20240124232823.png]]

![[Pasted image 20240124232854.png]]

```python
flightData2010.groupBy('DEST_COUNTRY_NAME').sum("count").withColumnRenamed("sum(count)", "destination_total").sort(desc("destination_total")).limit(5).explain()
```
![[Pasted image 20240124233150.png]]

# Chapter 3: A Tour of Spark's Toolset
Spark is composed of these primitives—the **lower-level APIs** and the  
**Structured APIs**—and then a series of standard libraries for additional functionality.
![[Pasted image 20240124233625.png]]

## Running Production Applications
Spark also makes it easy to turn your interactive exploration into production applications with `spark-submit`,  a built-in command-line tool. `spark-submit` does one thing: it lets you send your application code to a cluster and launch it to execute there. Upon submission, the application will run until it exits (completes the task) or encounters an error. You can do this with all of Spark’s support cluster managers including Standalone, Mesos, and  YARN.

## Datasets: Type-Safe Structured APIs
The APIs available on Datasets are *type-safe*, meaning that you cannot  
accidentally view the objects in a Dataset as being of another class than the class you put in initially.
Only works in Scala.

## Structured Streaming
With Structured Streaming, you can take the same  
operations that you perform in batch mode using Spark’s structured APIs and run them in a streaming fashion. This can reduce latency and allow for incremental processing.


