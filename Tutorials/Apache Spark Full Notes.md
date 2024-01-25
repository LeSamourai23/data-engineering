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
With Structured Streaming, you can take the same operations that you perform in batch mode using Spark’s structured APIs and run them in a streaming fashion. This can reduce latency and allow for incremental processing.
#### Example
```python
# Establish the DataFrame with all the files
staticDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema
```

Adding a total cost column and see on what days a customer spent the most.
The window function will include all data from each day in the aggregation. It’s simply a window over the time–series column in our data. This is a helpful tool for manipulating date and timestamps because we can specify our requirements in a more human form (via intervals), and Spark will group all of them together for us:
```python
from pyspark.sql.functions import window, column, desc, col

staticDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate").groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).sum("total_cost").show(5)
```
![[Pasted image 20240125131646.png]]

Now the Streaming Code. 
The biggest change is that we used `readStream` instead of read, additionally you’ll notice the `maxFilesPerTrigger` option, which simply specifies the number of files we should read in at once. This is to make our demonstration more “streaming,” and in a production scenario this would probably be omitted.
```python
streamingDataFrame = spark.readStream.schema(staticSchema).option("maxFilesPerTrigger", 1).format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/*.csv")
```

To check whether our DataFrame is streaming:
```python
streamingDataFrame.isStreaming    # Returns True
```
![[Pasted image 20240125132012.png]]

Now, we'll set up the same business logic as the previous DataFrame manipulation. We'll perform a summation in the process:
```python
purchaseByCustomerPerHour = streamingDataFrame.selectExpr("customerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate").groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).sum("total_cost")
```
![[Pasted image 20240125132147.png]]

This is still a lazy operation, so we will need to call a streaming action to start the execution of this data flow.
The action we will use will out‐ put to an in-memory table that we will update after each trigger. In this case, each trigger is based on an individual file (the read option that we set). Spark will mutate the data in the in-memory table such that we will always have the highest value as specified in our previous aggregation:
```python
purchaseByCustomerPerHour.writeStream.format("memory") # memory = store in-memory table
                                     .queryName("customer_purchases") # the name of the in-memory table
                                     .outputMode("complete") # complete = all the counts should be in the table
                                     .start()
```
![[Pasted image 20240125132328.png]]

When we start the stream, we can run queries against it to debug what our result will look like if we were to write this out to a production sink:
```python
spark.sql("""
          SELECT * FROM customer_purchases
          ORDER BY `sum(total_cost)`
          DESC""").show(5)
```
![[Pasted image 20240125132416.png]]

Another option you can use is to write the results out to the console:
```python
purchaseByCustomerPerHour.writeStream.format("console").queryName("customer_purchases_2").outputMode("complete").start()
```

## Machine Learning and Advanced Analytics
Another popular aspect of Spark is its ability to perform large-scale machine learning with a built-in library of machine learning algorithms called MLlib. MLlib allows for preprocessing, munging, training of models, and making predictions at scale on data.
We will perform some basic clustering on our data using a standard algorithm called *k-means*.

Machine learning algorithms in MLlib require that data is represented as numerical values. Our current data is represented by a variety of different types, including time‐ stamps, integers, and strings. Therefore we need to transform this data into some numerical representation:
```python
from pyspark.sql.functions import date_format, col

preppedDataFrame = staticDataFrame.na.fill(0).withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE")).coalesce(5)
```
![[Pasted image 20240125144123.png]]

We are also going to need to split the data into training and test sets. In this instance, we are going to do this manually by the date on which a certain purchase occurred; however, we could also use MLlib’s transformation APIs to create a training and test set via train validation splits or cross validation:
```python
trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")
```
![[Pasted image 20240125144144.png]]

Now that we’ve prepared the data, let’s split it into a training and test set. Because this is a time–series set of data, we will split by an arbitrary date in the dataset.
```python
trainDataFrame.count()
testDataFrame.count()
```
![[Pasted image 20240125144254.png]]

Spark’s MLlib also provides a number of transformations with which we can automate some of our general transformations. One such transformer is a `StringIndexer`:
```python
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")
```
This will turn our days of weeks into corresponding numerical values. For example, Spark might represent Saturday as 6, and Monday as 1. However, with this numbering scheme, we are implicitly stating that Saturday is greater than Monday (by pure numerical values). This is obviously incorrect.

To fix this, we therefore need to use a `OneHotEncoder` to encode each of these values as their own column. These Boolean flags state whether that day of week is the relevant day of the week:
```python
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded")
```

Each of these will result in a set of columns that we will “assemble” into a vector. All machine learning algorithms in Spark take as input a `Vector` type, which must be a set of numerical values:
```python
from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler().setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"]).setOutputCol("features")
```

Here, we have three key features: the price, the quantity, and the day of week. Next, we’ll set this up into a pipeline so that any future data we need to transform can go through the exact same process:
```python
from pyspark.ml import Pipeline

transformationPipeline = Pipeline().setStages([indexer, encoder, vectorAssembler])
```

Our `StringIndexer` needs to know how many unique values there are to be indexed. After those exist, encoding is easy but Spark must look at all the distinct values in the column to be indexed in order to store those values later on:
```python
fittedPipeline = transformationPipeline.fit(trainDataFrame)
```

After we fit the training data, we are ready to take that fitted **pipeline** and use it to transform all of our data in a consistent and repeatable way:
```python
transformedTraining = fittedPipeline.transform(trainDataFrame)
```
![[Pasted image 20240125145208.png]]

Establishing cache. This will put a copy of the intermediately transformed dataset into memory, allowing us to repeatedly access it at much lower cost than running the entire pipeline again.
```python
transformedTraining.cache()
```

We now have a training set; it’s time to train the model. First we’ll import the relevant model that we’d like to use and instantiate it:
```python
from pyspark.ml.clustering import KMeans

kmeans = KMeans().setK(20).setSeed(1)
```

In Spark, training machine learning models is a two-phase process. First, we initialize an untrained model, and then we train it. There are always two types for every algo‐ rithm in MLlib’s DataFrame API. They follow the naming pattern of **Algorithm**, for the untrained version, and **AlgorithmModel** for the trained version. In our example, this is `KMeans` and then `KMeansModel`.
```python
kmModel = kmeans.fit(transformedTraining)
```

After we train this model, we can compute the cost according to some success merits on our training set.
```python
kmModel.computeCost(transformedTraining)

transformedTest = fittedPipeline.transform(testDataFrame)
kmModel.computeCost(transformedTest)
```

## Lower-Level APIs
- Virtually everything in Spark is built on RDDs, but most users should stick to DataFrames for their simplicity and convenience.
- Use RDDs only when you need to do something that DataFrames cannot handle, such as manipulating raw data or customizing execution details.
- DataFrames are more user-friendly and abstract away the complex underlying mechanisms of RDDs.

One thing that you might use RDDs for is to parallelize raw data that you have stored in memory on the driver machine. For instance, let’s parallelize some simple numbers and create a DataFrame after we do so. We then can convert that to a DataFrame to use it with other DataFrames:
```python
from pyspark.sql import Row spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()
```

## Spark's Ecosystem and Packages
You can find the largest index of Spark Packages at [spark-packages.org].

# Chapter 4: Structured API Overview
The Structured APIs are a tool for manipulating all sorts of data, from unstructured log files to semistructured CSV files and highly structured Parquet files. These APIs refer to three core types of distributed collection APIs: 
• Datasets 
• DataFrames 
• SQL tables and views

## DataFrames and Datasets
* Spark has two notions of structured collections: **DataFrames** and **Datasets**. 
* **DataFrame** and **Datasets** are (distributed) table-like collections with well-defined rows and columns. Each column must have the same number of rows as all the other columns (although you can use null to specify the absence of a value) and each column has type information that must be consistent for every row in the collection.
* To Spark, **DataFrames** and **Datasets** represent immutable, lazily evaluated plans that specify what operations to apply to data residing at a location to generate some output. When we perform an action on a **DataFrame**, we instruct Spark to perform the actual transformations and return the result. These represent plans of how to manipulate rows and columns to compute the user’s desired result.
* Tables and views are basically the same thing as DataFrames. We just execute SQL against them instead of DataFrame code.

## Overview of Structured Spark Types
Spark is effectively a programming language of its own. Internally, Spark uses an engine called **Catalyst** that maintains its own type information through the planning and processing of work.

## DataFrames Versus Datasets
Types within Structured APIs:
- **DataFrames:**
    - **Type system:** Considered "untyped" because Spark manages and infers types behind the scenes based on the schema at runtime. It checks for consistency, but not against explicit type declarations.
    - **Availability:** Available in all Spark languages (Scala, Java, Python, R).
    - **Internal representation:** In Scala, Spark represents DataFrames as Datasets of type "Row", a specialized format optimized for efficient in-memory computations. This avoids costly garbage collection and object creation associated with JVM types.
    - **In Python and R:** Everything is implicitly translated to this optimized DataFrame format, eliminating the need for a separate Dataset concept.
    
- **Datasets:**
    - **Type system:** "Typed" because it relies on explicit type declarations (case classes in Scala, Java beans in Java) that are checked for compliance at compile time. This provides static type safety and catches errors early.
    - **Availability:** Limited to JVM-based languages (Scala and Java) due to its reliance on compile-time checks.

**Key Takeaways:**

- In most cases, **DataFrames** are preferred for their flexibility and availability across all languages.
- **Datasets** offer stronger type safety through compile-time checks, but are limited to JVM languages.
- Both **DataFrames** and **Datasets** leverage Spark's optimized internal format for efficient computations.
- This internal format avoids JVM object creation and garbage collection overhead, leading to better performance.

## Columns
Columns represent a *simple type* like an integer or string, a complex type like an array or map, or a *null value*. Spark tracks all of this type information for you and offers a variety of ways, with which you can transform columns.

## Rows
A row is nothing more than a record of data. Each record in a DataFrame must be of type Row, as we can see when we collect the following DataFrames.
```python
spark.range(2).collect()
```

## Spark Types
Before getting to those tables, let’s talk about how we instantiate, or declare, a column to be of a certain type. To work with the correct Scala types, use the following:
```python
from pyspark.sql.types import *
b = ByteType()
```

![[Pasted image 20240125153536.png]]
![[Pasted image 20240125153546.png]]

## Overview of Structured API Execution
This section will demonstrate how this code is actually executed across a cluster. This will help you understand (and potentially debug) the process of writing and executing code on clusters, so let’s walk through the execution of a single structured API query from user code to executed code:

1. Write DataFrame/Dataset/SQL Code.
2. If valid code, Spark converts this to a *Logical Plan*.
3. Spark transforms this *Logical Plan* to a *Physical Plan*, checking for optimizations along the way.
4. Spark then executes this *Physical Plan* (RDD manipulations) on the cluster.

![[Pasted image 20240125153900.png]]

## Logical Planning
The first phase of execution is meant to take user code and convert it into a logical plan.
![[Pasted image 20240125153945.png]]

* **Unresolved Logical Plan:** Convert user code to a plan (tables/columns might not exist yet).
* **Analysis:** Check if referenced tables/columns exist in the catalog.
    
    - Reject plan if references are invalid.
    - Resolve plan if references are valid.
    
* **Optimization:** Apply Catalyst Optimizer rules to improve the plan.
    - Push down predicates or selections.
    - Extensions possible for domain-specific optimizations.

## Physical Planning
After successfully creating an optimized logical plan, Spark then begins the physical planning process. The physical plan, often called a Spark plan, specifies how the logical plan will execute on the cluster by generating different physical execution strategies and comparing them through a cost model.

![[Pasted image 20240125154844.png]]
Physical planning results in a series of RDDs and transformations.

## Execution
Upon selecting a physical plan, Spark runs all of this code over RDDs, the lower-level programming interface of Spark. Spark performs further optimizations at runtime, generating native Java bytecode that can remove entire tasks or stages during execution. Finally the result is returned to the user.