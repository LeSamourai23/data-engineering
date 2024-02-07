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
# Chapter 5: Basic Structured Operations
A DataFrame consists of a series of records (like rows in a table), that are of type Row, and a number of columns (like columns in a spreadsheet) that represent a computation expression that can be performed on each individual record in the Dataset. Schemas define the name as well as the type of data in each column. Partitioning of the DataFrame defines the layout of the DataFrame or Dataset’s physical distribution across the cluster. The partitioning scheme defines how that is allocated.
## Schemas
```python
df.schema()
```
![[Pasted image 20240128221157.png]]

A schema is a `StructType` made up of a number of fields, `StructFields`, that have a name, type, a Boolean flag which specifies whether that column can contain missing or null values, and, finally, users can optionally specify associated metadata with that column.
If the types in the data (at runtime) do not match the schema, Spark will throw an error.
The example that follows shows how to create and enforce a specific schema on a DataFrame:
```python
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
StructField("DEST_COUNTRY_NAME", StringType(), True),
StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
StructField("count", LongType(), False, metadata={"hello":"world"})
])
  
df = spark.read.format("json").schema(myManualSchema).load("dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/2015_summary.json")
```
![[Pasted image 20240128221550.png]]

## Columns and Expressions
To Spark, columns are logical constructions that simply represent a value computed  
on a per-record basis by means of an expression. This means that to have a real value  
for a column, we need to have a row; and to have a row, we need to have a Data‐  
Frame.

## Columns
There are a lot of different ways to construct and refer to columns but the two sim‐  
plest ways are by using the col or column functions. To use either of these functions,  
you pass in a column name:
```python
from pyspark.sql.functions import col, column  

col("someColumnName")  
column("someColumnName")
```
#### Explicit column references
If you need to refer to a specific DataFrame’s column, you can use the col method on  
the specific DataFrame.
```python
df.col("count")
```

## Expressions
An expression is a set of transformations on one or more values in a record in a **DataFrame**. Think of it like a function that takes as input one or more column names, resolves them, and then potentially applies more expressions to create a single value  
for each record in the dataset. Importantly, this “single value” can actually be a com‐  
plex type like a `Map` or `Array`.
#### Columns as expressions
If you use `col()` and want to perform transformations on that column, you must perform those on that column reference. When using an expression, the expr function can actually parse transformations and column references from a string and can subsequently be passed into further transformations.

`expr("someCol - 5")` is the same transformation as performing `col("someCol") - 5`, or even `expr("someCol") - 5`. That’s because Spark compiles these to a logical tree  
specifying the order of operations.
Example:
```
(((col("someCol") + 5) * 200) - 6) < col("otherCol")
```
![[Pasted image 20240128230528.png]]

```python
from pyspark.sql.functions import expr  

expr("(((someCol + 5) * 200) - 6) < otherCol")
```
#### Accessing a DataFrame's columns
```
spark.read.format("json").load("/data/flight-data/json/2015-summary.json").columns
```

## Records and Rows
In Spark, each row in a DataFrame is a single record. Spark represents this record as  
an object of type **Row**. Spark manipulates **Row** objects using column expressions in  
order to produce usable values. **Row** objects internally represent arrays of `bytes`.
#### Creating Rows
```python
from pyspark.sql import Row  

myRow = Row("Hello", None, 1, False)
```

For accessing rows:
```python
myRow[2]
```
![[Pasted image 20240128231129.png]]
## DataFrame Transformations
• We can add rows or columns  
• We can remove rows or columns  
• We can transform a row into a column (or vice versa)  
• We can change the order of rows based on the values in columns

![[Pasted image 20240128231233.png]]
#### Creating DataFrames
```python
df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")  
df.createOrReplaceTempView("dfTable")
  
df.show()
```
![[Pasted image 20240128235019.png]]

or you can also make **DataFrames** by taking a set of rows and converting them  
to a **DataFrame**:
```python
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
  
myManualSchema = StructType([
StructField("some", StringType(), True),
StructField("col", StringType(), True),
StructField("names", LongType(), False)
])

myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
  
myDf.show()
```
![[Pasted image 20240128235604.png]]
#### select and selectExpr
select and selectExpr allow you to do the DataFrame equivalent of SQL queries on  
a table of data:
```SQL
SELECT * FROM dataFrameTable  
SELECT columnName FROM dataFrameTable  
SELECT columnName * 10, otherColumn, someOtherCol as c FROM dataFrameTable
```

In the simplest possible terms, you can use them to manipulate columns in your  
DataFrames.
For Example:
```python
df.select("DEST_COUNTRY_NAME").show(2)
```
![[Pasted image 20240129000018.png]]

You can select multiple columns by using the same style of query, just add more column name strings to your select method call:
```python
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
```
![[Pasted image 20240129000105.png]]

```python
from pyspark.sql.functions import expr, col, column  

df.select(  
expr("DEST_COUNTRY_NAME"),  
col("DEST_COUNTRY_NAME"),  
column("DEST_COUNTRY_NAME")).show(2)
```
![[Pasted image 20240129140555.png]]

Now, for using aliases:
```python
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
```
![[Pasted image 20240129173020.png]]

we can also do further manipulation by:
```python
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)
```
![[Pasted image 20240129173041.png]]
This changes the column back to its original name.

Because `select` followed by a series of expr is such a common pattern, Spark has a shorthand for doing this efficiently: `selectExpr`. This is probably the most convenient interface for everyday use:
```python
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
```
![[Pasted image 20240129173156.png]]

We can treat `selectExpr` as a simple way to build up complex expressions that create new DataFrames. In fact, we can add any valid non-aggregating SQL statement, and as long as the columns resolve, it will be valid:
```python
df.selectExpr(
"*", # all original columns
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)
```
![[Pasted image 20240129173316.png]]

We can also specify aggregations over the entire DataFrame by taking advantage of the functions that we have:
```python
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
```
![[Pasted image 20240129173443.png]]
#### Converting to Spark Types (Literals)
A translation from a given programming language’s literal value to one that Spark understands. Literals are expressions and you can use them in the same way:
```python
from pyspark.sql.functions import lit  

df.select(expr("*"), lit(1).alias("One")).show(2)
```
![[Pasted image 20240129192127.png]]

This will come up when you might need to check whether a value is greater than  
some constant or other programmatically created variable.
#### Adding Columns
There’s also a more formal way of adding a new column to a DataFrame, and that’s by  
using the `withColumn` method on our DataFrame:
```python
df.withColumn("numberOne", lit(1)).show(2)
```
![[Pasted image 20240129192441.png]]

In the next example, we’ll set a Boolean flag for when the origin country is the same as the destination country:
```python
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
```
![[Pasted image 20240129192526.png]]

the `withColumn` function takes two arguments: the column name and the  
expression that will create the value for that given row in the DataFrame.
#### Renaming Columns
Use the `withColumnRenamed` method. This will rename the column with the name of the string in the first argument to the string in the second argument:
![[Pasted image 20240129193213.png]]
#### Reserved Characters and Keywords
One thing that you might come across is reserved characters like spaces or dashes in  
column names. Handling these means escaping column names appropriately. In  
Spark, we do this by using **backtick** character.
```python
dfWithLongColName = df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
```

We don’t need escape characters here because the first argument to `withColumn` is just 
a string for the new column name. In this example, however, we need to use backticks  
because we’re referencing a column in an expression:
```python
dfWithLongColName.selectExpr(  
"`This Long Column-Name`",  
"`This Long Column-Name` as `new col`").show(2)  

dfWithLongColName.createOrReplaceTempView("dfTableLong")
```
#### Case Sensitivity
By default Spark is case insensitive; however, you can make Spark case sensitive by  
setting the configuration:
```SQL
SET spark.sql.caseSensitive true
```
#### Removing Columns
```python
df.drop("ORIGIN_COUNTRY_NAME").columns
```

Remove multiple columns by:
```python
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
```
#### Changing a Column's Type (cast)
Sometimes, we might need to convert from one type to another; for example, if we  
have a set of `StringType` that should be integers. We can convert columns from one  
type to another by casting the column from one type to another. For instance, let’s  
convert our count column from an integer to a type `Long`:
```python
df.withColumn("count2", col("count").cast("long"))
```
#### Filtering Rows
There are two methods to perform this operation: you can use where or filter and they both will perform the same operation and accept the same argument types when used with DataFrames.
```python
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
```
![[Pasted image 20240129195536.png]]

If you want to specify multiple `AND` filters, just chain them sequentially  
and let Spark handle the rest:
```python
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show(2)
```
![[Pasted image 20240129200204.png]]
#### Getting Unique Rows
A very common use case is to extract the unique or distinct values in a DataFrame.  
These values can be in one or more columns. The way we do this is by using the  
`distinct` method on a DataFrame, which allows us to deduplicate any rows that are  
in that DataFrame.
```python
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
```
![[Pasted image 20240129200705.png]]
#### Random Samples
You can do this by using the `sample` method on a DataFrame, which makes it possible for you to specify a fraction of rows to extract from a DataFrame and whether you’d like to sample with or without replacement:
```python
seed = 5
withReplacement = False
fraction = 0.5

df.sample(withReplacement, fraction, seed).count()
```
![[Pasted image 20240129200843.png]]
#### Random Splits
Random splits can be helpful when you need to break up your DataFrame into a random “splits” of the original DataFrame. This is often used with machine learning  
algorithms to create training, validation, and test sets. 
```python
dataFrames = df.randomSplit([0.25, 0.75], seed)  
dataFrames[0].count() > dataFrames[1].count() # False
```
#### Concatenating and Appending Rows (Union)
To append to a DataFrame, you must `union` the original DataFrame along with the new DataFrame. This just concatenates the two DataFrames.
```python
from pyspark.sql import Row

schema = df.schema
newRows = [
Row("New Country", "Other Country", 5L),
Row("New Country 2", "Other Country 3", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)

newDF = spark.createDataFrame(parallelizedRows, schema)
```
![[Pasted image 20240129201700.png]]
#### Sorting Rows
There are two equivalent operations to do this `sort` and `orderBy` that work the exact same way. They accept both column expressions and strings as well as multiple columns. The default is to sort in ascending order:
```python
df.sort("count").show(5)

df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
```
![[Pasted image 20240129203408.png]]
![[Pasted image 20240129203433.png]]

To more explicitly specify sort direction, you need to use the asc and desc functions  
if operating on a column:
```python
from pyspark.sql.functions import desc, asc  

df.orderBy(expr("count desc")).show(2)  
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)
```
![[Pasted image 20240129203525.png]]
An advanced tip is to use `asc_nulls_first`, `desc_nulls_first`, `asc_nulls_last`, or  
`desc_nulls_last` to specify where you would like your null values to appear in an  
ordered DataFrame.

For optimization purposes, it’s sometimes advisable to sort within each partition  
before another set of transformations. You can use the `sortWithinPartitions` 
method to do this:
```python
spark.read.format("json").load("/data/flight-data/json/*-summary.json").sortWithinPartitions("count")
```
#### Limit
Oftentimes, you might want to restrict what you extract from a DataFrame; for example, you might want just the top ten of some DataFrame. You can do this by using the `limit` method:
```python
df.limit(5).show()
```
![[Pasted image 20240129203728.png]]
```python
df.orderBy(expr("count desc")).limit(6).show()
```
![[Pasted image 20240129203749.png]]
#### Repartition and Coalesce
Repartition will incur a full shuffle of the data, regardless of whether one is necessary. This means that you should typically only repartition when the future number of partitions is greater than your current number of partitions or when you are looking to  
partition by a set of columns:
```python
df.rdd.getNumPartitions() # 1
```

If you know that you’re going to be filtering by a certain column often, it can be  
worth repartitioning based on that column:
```python
df.repartition(col("DEST_COUNTRY_NAME"))
```

You can optionally specify the number of partitions you would like, too:
```python
df.repartition(5, col("DEST_COUNTRY_NAME"))
```

Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions. This operation will shuffle your data into five partitions based on the destination country name, and then coalesce them (without a full shuffle):
```python
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
```

# Chapter 6: Working with Different Types of Data
There are a variety of different kinds of data, including: 
• Booleans  
• Numbers  
• Strings  
• Dates and timestamps  
• Handling null  
• Complex types  
• User-defined functions
## Converting to Spark Types
The `lit` function converts a type in another language to its correspnding Spark representation:
```python
from pyspark.sql.functions import lit  

df.select(lit(5), lit("five"), lit(5.0))
```
![[Pasted image 20240130135503.png]]
## Working with Booleans
```python
from pyspark.sql.functions import col

df.where(col("InvoiceNo") != 536365).select("InvoiceNo", "Description").show(5, False)
```
![[Pasted image 20240130135626.png]]

Another option and probably the cleanest is to specify the predicate as an expression in a string:
```python
df.where("InvoiceNo = 536365").show(5, False)  
```
![[Pasted image 20240130135858.png]]
```python
df.where("InvoiceNo <> 536365").show(5, False)
```
![[Pasted image 20240130135931.png]]

The reason for this is that even if Boolean statements are expressed serially (one after the other), Spark will flatten all of these filters into one statement and perform the filter at the same time, creating the `and` statement for us.
```python
from pyspark.sql.functions import instr  

priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1  

df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()
```
![[Pasted image 20240130140341.png]]

Boolean expressions are not just reserved to filters. To filter a DataFrame, you can also  
just specify a Boolean column:
```python
from pyspark.sql.functions import instr   

DOTCodeFilter = col("StockCode") == "DOT"  
priceFilter = col("UnitPrice") > 600  
descripFilter = instr(col("Description"), "POSTAGE") >= 1  
  
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)).where("isExpensive").select("unitPrice", "isExpensive").show(5)
```
![[Pasted image 20240130141937.png]]

it’s often easier to just express filters as SQL statements than using the programmatic DataFrame interface and Spark SQL allows us to do this without paying any performance penalty:
```python
from pyspark.sql.functions import expr  

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250")).where("isExpensive").select("Description", "UnitPrice").show(5)
```
![[Pasted image 20240130142741.png]]
## Working with Numbers
To fabricate a contrived example, let’s imagine that we found out that we misrecorded the quantity in our retail dataset and the true quantity is equal to

$(\text{the current quantity}  *  \text{the  unit  price})^2 + 5$

This will introduce our first numerical function as well as the pow function that raises a column to the expressed power:
```python
from pyspark.sql.functions import expr, pow  

fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5  
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
```
![[Pasted image 20240130175715.png]]

Naturally we can add and subtract as necessary, as well. In fact, we can do  
all of this as a SQL expression, as well:
```python
df.selectExpr(  
"CustomerId",  
"(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
```
![[Pasted image 20240130175707.png]]

Next up, **rounding**:
```python
from pyspark.sql.functions import lit, round, bround

df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
```
![[Pasted image 20240130175826.png]]

Another numerical task is to compute the correlation of two columns. For example,  
we can see the Pearson correlation coefficient for two columns to see if cheaper things  
are typically bought in greater quantities:
```python
from pyspark.sql.functions import corr  

df.stat.corr("Quantity", "UnitPrice")  
df.select(corr("Quantity", "UnitPrice")).show()
```
![[Pasted image 20240130181701.png]]

Another common task is to compute summary statistics for a column or set of columns. We can use the `describe` method to achieve exactly this:
```python
df.describe().show()
```
![[Pasted image 20240130181809.png]]

If you need these exact numbers, you can also perform this as an aggregation yourself  
by importing the functions and applying them to the columns that you need:
```python
from pyspark.sql.functions import count, mean, stddev_pop, min, max
```

To calculate either exact or approximate quantiles of your data using the `approxQuantile` method:
```python
colName = "UnitPrice"  
quantileProbs = [0.5]  
relError = 0.05  
df.stat.approxQuantile("UnitPrice", quantileProbs, relError)
```
![[Pasted image 20240130182000.png]]

You also can use this to see a cross-tabulation or frequent item pairs:
```python
df.stat.crosstab("StockCode", "Quantity").show()
```
![[Pasted image 20240130182112.png]]

we can also add a unique ID to each row by using the function `monotonically_increasing_id`. This function generates a unique value for each row, starting with 0:
```python
from pyspark.sql.functions import monotonically_increasing_id  

df.select(monotonically_increasing_id()).show(2)
```
![[Pasted image 20240130182253.png]]

There are some random data generation tools (e.g., `rand()`, `randn()`) with which you can randomly generate data; however, there are potential determinism issues when doing so.
## Working with Strings
The `initcap` function will capitalize every word in a given string when that word is separated from another by a space:
```python 
from pyspark.sql.functions import initcap  

df.select(initcap(col("Description"))).show()
```
![[Pasted image 20240130182931.png]]

As just mentioned, you can cast strings in uppercase and lowercase, as well:
```python
from pyspark.sql.functions import lower, upper  

df.select(col("Description"),  
lower(col("Description")),  
upper(lower(col("Description")))).show(2)
```
![[Pasted image 20240130183027.png]]

Another trivial task is adding or removing spaces around a string. You can do this by using `lpad`, `ltrim`, `rpad` and `rtrim`, `trim`:
```python
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim  

df.select(  
ltrim(lit(" HELLO ")).alias("ltrim"),  
rtrim(lit(" HELLO ")).alias("rtrim"),  
trim(lit(" HELLO ")).alias("trim"),  
lpad(lit("HELLO"), 3, " ").alias("lp"),  
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)
```
![[Pasted image 20240130183157.png]]

#### Regular Expressions
Probably one of the most frequently performed tasks is searching for the existence of one string in another or replacing all mentions of a string with another value. This is often done with a tool called `regular expressions`.
```python
from pyspark.sql.functions import regexp_replace  

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"  
df.select(  
regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"), col("Description")).show(2)
```
![[Pasted image 20240131140358.png]]

Another task might be to replace given characters with other characters. Building this  
as a regular expression could be tedious, so Spark also provides the translate function  
to replace these values. This is done at the character level and will replace all instances  
of a character with the indexed character in the replacement string:
```python
from pyspark.sql.functions import translate  

df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\  
.show(2)
```
![[Pasted image 20240131141405.png]]

We can also perform something similar, like pulling out the first mentioned color:
```python
from pyspark.sql.functions import regexp_extract  

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"  
df.select(  
regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),  
col("Description")).show(2)
```
![[Pasted image 20240131141450.png]]

Sometimes, rather than extracting values, we simply want to check for their existence.  
We can do this with the `contains` method on each column:
```python
from pyspark.sql.functions import instr  

containsBlack = instr(col("Description"), "BLACK") >= 1  
containsWhite = instr(col("Description"), "WHITE") >= 1  

df.withColumn("hasSimpleColor", containsBlack | containsWhite).where("hasSimpleColor").select("Description").show(3, False)
```
![[Pasted image 20240131141624.png]]

Spark’s ability to accept a dynamic number of arguments. When we convert a list of values into a set of arguments and pass them into a function, we use a language feature called `var args`. Using this feature, we can effectively unravel an array of arbitrary length and pass it as arguments to a function. This, coupled with `select` makes it possible for us to create arbitrary numbers of columns dynamically. In Python, we’re going to use a different function, `locate`, that returns the integer location (1 based location):
```python
from pyspark.sql.functions import expr, locate  
simpleColors = ["black", "white", "red", "green", "blue"]  

def color_locator(column, color_string):  
	return locate(color_string.upper(), column).cast("boolean").alias("is_" + c)  

selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type  

df.select(*selectedColumns).where(expr("is_white OR is_red")).select("Description").show(3, False)
```
## Working with Dates and Timestamps
Create a basic date table:
```python
from pyspark.sql.functions import current_date, current_timestamp

dateDF = spark.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()
```
![[Pasted image 20240131142628.png]]

Add and subtract five days from today. These functions take a column and then the number of days to either add or subtract as the arguments:
```python
from pyspark.sql.functions import date_add, date_sub  

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
```
![[Pasted image 20240131142825.png]]

Another common task is to take a look at the difference between two dates. We can do this with the `datediff` function that will return the number of days in between two dates:
```python
from pyspark.sql.functions import datediff, months_between, to_date  

dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(datediff(col("week_ago"), col("today"))).show(1)  
dateDF.select(  
to_date(lit("2016-01-01")).alias("start"),  
to_date(lit("2017-05-22")).alias("end")).select(months_between(col("start"), col("end"))).show(1)
```
![[Pasted image 20240131143014.png]]

The `to_date` function allows you to convert a string to a date, optionally with a specified format:
```python
from pyspark.sql.functions import to_date, lit  

spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).show(1)
```

Spark will not throw an error if it cannot parse the date; rather, it will just return  
*null*. This can be a bit tricky in larger pipelines because you might be expecting your  
data in one format and getting it in another.
```python
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)
```
![[Pasted image 20240131143432.png]]

To fix this pipeline, step by step, and come up with a robust way to avoid these issues entirely. The first step is to remember that we need to specify our date format according to the **Java SimpleDateFormat standard**.  
We will use two functions to fix this: `to_date` and `to_timestamp`. The former option‐  
ally expects a format, whereas the latter requires one:
```python
from pyspark.sql.functions import to_date

dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")
```
![[Pasted image 20240131143659.png]]
Now let’s use an example of `to_timestamp`, which always requires a format to be  
specified:
```python
from pyspark.sql.functions import to_timestamp  

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
```
![[Pasted image 20240131143733.png]]

After we have our date or timestamp in the correct format and type, comparing  
between them is actually quite easy. We just need to be sure to either use a date/time‐  
stamp type or specify our string according to the right format of *yyyy-MM-dd* if we’re  
comparing a date:
```python
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
```
## Working with Nulls in Data
There are two things you can do with null values: you can explicitly **drop** nulls or you can **fill them** with a value (globally or on a per-column basis). 
#### Coalesce
Spark includes a function to allow you to select the first non-null value from a set of  
columns by using the `coalesce` function. In this case, there are no null values, so it simply returns the first column:
```python
from pyspark.sql.functions import coalesce  

df.select(coalesce(col("Description"), col("CustomerId"))).show()
```
![[Pasted image 20240131144348.png]]
#### ifnull, nullif, nvl and nvl2
`ifnull` allows you to select the second value if the first is null, and defaults to the first. Alternatively, you could use `nullif`, which returns null if the two values are equal or else returns the second if they are not. `nvl` returns the second value if the first is null, but defaults to the first. Finally, `nvl2` returns the second value if the first is not null; otherwise, it will return the last specified value (`else_value` in the following example):
```SQL
%sql

SELECT
ifnull(null, 'return_value'),
nullif('value', 'value'),
nvl(null, 'return_value'),
nvl2('not_null', 'return_value', "else_value")
FROM dfTable LIMIT 1
```
![[Pasted image 20240131144629.png]]
#### drop
The simplest function is `drop`, which removes rows that contain *nulls*. The default is  
to drop any row in which any value is *null*:
```python
df.na.drop()  
df.na.drop("any")
```

Specifying "any" as an argument drops a row if any of the values are null. Using “all”  
drops the row only if all values are *null* or *NaN* for that row:
```python
df.na.drop("all")
```

We can also apply this to certain sets of columns by passing in an array of columns:
```python
df.na.drop("all", subset=["StockCode", "InvoiceNo"])
```

#### fill
Using the `fill` function, you can fill one or more columns with a set of values. This can be done by specifying a map—that is a particular value and a set of columns.
To fill all null values in columns of type String, you might specify the following:
```python
df.na.fill("All Null values become this string")
```

We could do the same for columns of type Integer by using `df.na.fill(5:Integer)`,  
or for Doubles `df.na.fill(5:Double)`. To specify columns, we just pass in an array of column names like we did in the previous example:
```python
df.na.fill("all", subset=["StockCode", "InvoiceNo"])
```

We can also do this with with a Scala Map, where the key is the column name and the value is the value we would like to use to fill null values:
```python
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}  

df.na.fill(fill_cols_vals)
```
#### replace
to replace all values in a certain column according to their current value:
```python
df.na.replace([""], ["UNKNOWN"], "Description")
```
#### ordering
You can use `asc_nulls_first`, `desc_nulls_first`, `asc_nulls_last`, or `desc_nulls_last` to specify where you would like your null values to appear in an ordered DataFrame.
## Working with Complex Types
### Structs
You can think of structs as DataFrames within DataFrames. We can create a struct by wrapping a set of columns in parenthesis in a query:
```python
df.selectExpr("(Description, InvoiceNo) as complex", "*")  
df.selectExpr("struct(Description, InvoiceNo) as complex", "*")
```
```python
from pyspark.sql.functions import struct

complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")
```
![[Pasted image 20240201000427.png]]

We now have a DataFrame with a column complex. We can query it just as we might  
another DataFrame, the only difference is that we use a dot syntax to do so, or the  
column method `getField`:
```python
complexDF.select("complex.Description")  
complexDF.select(col("complex").getField("Description"))
```

We can also query all values in the struct by using `*`. This brings up all the columns to  
the top-level DataFrame:
```python
complexDF.select("complex.*complexDF.select("complex.*")
```
### Arrays
Our objective is to take every single word in our Description column and convert that into a row in our DataFrame.  
#### split
The first task is to turn our Description column into a complex type, an array. We do this by using the `split` function.
```python
from pyspark.sql.functions import split  

df.select(split(col("Description"), " ")).show(2)
```
![[Pasted image 20240201001444.png]]

We can also query the values of the array using Python-like syntax:
```python
df.select(split(col("Description"), " ").alias("array_col")).selectExpr("array_col[0]").show(2)
```
#### array_contains
We can also see whether this array contains a value:
```python
from pyspark.sql.functions import array_contains  

df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
```
![[Pasted image 20240201014329.png]]
#### explode
The explode function takes a column that consists of arrays and creates one row (with the rest of the values duplicated) per value in the array.
![[Pasted image 20240201014633.png]]

```python
from pyspark.sql.functions import split, explode  

df.withColumn("splitted", split(col("Description"), " "))
withColumn("exploded", explode(col("splitted"))).select("Description", "InvoiceNo", "exploded").show(2)
```
![[Pasted image 20240201015206.png]]
#### Maps
Maps are created by using the map function and key-value pairs of columns. You then can select them just like you might select from an array:
```python
from pyspark.sql.functions import create_map  
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)
```


# Chapter 7: Aggregations
Intro
# Chapter 8: Joins
## Join Expressions
A join brings together two sets of data, the left and the right, by comparing the value of one or more keys of the left and right and evaluating the result of a join expression that determines whether Spark should bring together the left set of data with the right set of data.
## Join Types
* **Inner Join**: keep rows with keys that exist in the left and right datasets.
* **Outer Joins**: keep rows with keys in either the left or right datasets
* **Left Outer Join**: keep rows with keys in the left dataset
* **Right Outer Join**: keep rows with keys in the right dataset
* **Left Semi Join**: keep the rows in the left, and only the left, dataset where the key appears in the right dataset.
* **Left Anti Join**: keep the rows in the left, and only the left, dataset where they do not appear in the right dataset.
* **Natural Join**: perform a join by implicitly matching the columns between the two datasets with the same names.
* **Cross Join (Cartesian Join)**: (match every row in the left dataset with every row in the right dataset)

Lets start with an example dataset:
```python
person = spark.createDataFrame([ (0, "Bill Chambers", 0, [100]),
								(1, "Matei Zaharia", 1, [500, 250, 100]), (2, "Michael Armbrust", 1, [250, 100])]).toDF("id", "name", "graduate_program", "spark_status")
								
graduateProgram = spark.createDataFrame([ (0, "Masters", "School of Information", "UC Berkeley"), (2, "Masters", "EECS", "UC Berkeley"), (1, "Ph.D.", "EECS", "UC Berkeley")]).toDF("id", "degree", "department", "school") 

sparkStatus = spark.createDataFrame([ (500, "Vice President"), (250, "PMC Member"), (100, "Contributor")]).toDF
```

Register the tables:
```python
person.createOrReplaceTempView("person") graduateProgram.createOrReplaceTempView("graduateProgram") sparkStatus.createOrReplaceTempView("sparkStatus")
```
![[Pasted image 20240201114725.png]]
![[Pasted image 20240201114737.png]]
![[Pasted image 20240201114748.png]]
## Inner Joins
In the following example, we join the `graduateProgram` DataFrame with the `person` DataFrame to create a new DataFrame:
```python
joinExpression = person["graduate_program"] == graduateProgram['id']
```
![[Pasted image 20240201114939.png]]

Keys that do not exist in both DataFrames will not show in the resulting DataFrame. For example, the following expression would result in zero values in the resulting DataFrame:
```python
wrongJoinExpression = person["name"] == graduateProgram["school"]
```

**Inner Joins** are the default join, so we just need to specify our left DataFrame and join the right in the JOIN expression:
```python
person.join(graduateProgram, joinExpression).show()
```
![[Pasted image 20240201115147.png]]

We can also specify this explicitly by passing in a third parameter, the `joinType`:
```python
joinType = "inner" 
person.join(graduateProgram, joinExpression, joinType).show()
```
![[Pasted image 20240201115312.png]]
## Outer Joins
**Outer joins** evaluate the keys in both of the DataFrames or tables and includes (and joins together) the rows that evaluate to true or false. If there is no equivalent row in either the left or right DataFrame, Spark will insert *null*:
```python
joinType = "outer" 
person.join(graduateProgram, joinExpression, joinType).show()
```
![[Pasted image 20240201115454.png]]
## Left Outer Joins
**Left outer joins** evaluate the keys in both of the DataFrames or tables and includes all rows from the left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert `null`:
```python
joinType = "left_outer" 
graduateProgram.join(person, joinExpression, joinType).show()
```
![[Pasted image 20240201115611.png]]
## Right Outer Joins
**Right outer joins** evaluate the keys in both of the DataFrames or tables and includes all rows from the right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert `null`:
```python
joinType = "right_outer" 
person.join(graduateProgram, joinExpression, joinType).show()
```
![[Pasted image 20240201115727.png]]
## Left Semi Joins
**Semi joins** are a bit of a departure from the other joins. They do not actually include any values from the right DataFrame. They only compare values to see if the value exists in the second DataFrame. If the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left DataFrame:
```python
joinType = "left_semi" 
graduateProgram.join(person, joinExpression, joinType).show()
```
![[Pasted image 20240201120125.png]]
```python
gradProgram2 = graduateProgram.union(spark.createDataFrame([ (0, "Masters", "Duplicated Row", "Duplicated School")])) 
gradProgram2.createOrReplaceTempView("gradProgram2") 

gradProgram2.join(person, joinExpression, joinType).show()
```
![[Pasted image 20240201120216.png]]
## Left Anti Joins
**Left anti joins** are the ***opposite*** of **left semi joins**. they do not actually include any values from the right DataFrame. They only compare values to see if the value exists in the second DataFrame. However, rather than keeping the values that exist in the second DataFrame, they keep only the values that do not have a corresponding key in the second DataFrame:
```python
joinType = "left_anti" 
graduateProgram.join(person, joinExpression, joinType).show()
```
![[Pasted image 20240201120426.png]]
## Natural Joins
**Natural joins** make implicit guesses at the columns on which you would like to join:
```python
joinType: "natural"
graduateProgram.join(person, joinExpression, joinType).show()
```
![[Pasted image 20240201120555.png]]
## Cross (Cartesian) Joins

The last of our joins are **cross-joins** or cartesian products. **Cross-joins** in simplest terms are inner joins that do not specify a predicate. **Cross joins** will join every single row in the left DataFrame to ever single row in the right DataFrame. You must very explicitly state that you want a cross-join by using the cross join keyword:
```python
joinType = "cross" 
graduateProgram.join(person, joinExpression, joinType).show()
```
![[Pasted image 20240201120720.png]]

If you truly intend to have a cross-join, you can call that out explicitly:
```python
person.crossJoin(graduateProgram).show()
```
## Challenges when using Joins
#### Joins on Complex Types
Even though this might seem like a challenge, it’s actually not. Any expression is a valid join expression, assuming that it returns a Boolean:
```python
from pyspark.sql.functions import expr person.withColumnRenamed("id", "personId").join(sparkStatus, expr("array_contains(spark_status, id)")).show()
```
![[Pasted image 20240201121224.png]]
#### Handling Duplicate Column Names
One of the tricky things that come up in joins is dealing with duplicate column names in your results DataFrame. In a DataFrame, each column has a unique ID within Spark’s SQL Engine, Catalyst. This makes it quite difficult to refer to a specific column when you have a DataFrame with duplicate column names.

This can occur in two distinct situations: 
• The join expression that you specify does not remove one key from one of the input DataFrames and the keys have the same column name.
• Two columns on which you are not performing the join have the same name.

Example:
```python
gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
joinExpr = gradProgramDupe["graduate_program"] == person["graduate_program"]

person.join(gradProgramDupe, joinExpr).show()
```
![[Pasted image 20240201122510.png]]
There are now two `graduate_program` columns, even though we joined on that key. The challenge arises when we refer to one of these columns:
```python
person.join(gradProgramDupe, joinExpr).select("graduate_program").show()
```
![[Pasted image 20240201122641.png]]
##### Approach 1: Different join expression
When you have two keys that have the same name, probably the easiest fix is to change the join expression from a Boolean expression to a string or sequence. This automatically removes one of the columns for you during the join:
```python
person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
```
![[Pasted image 20240201122753.png]]
##### Approach 2: Dropping the column after the join
Another approach is to drop the offending column after the join. When doing this, we need to refer to the column via the original source DataFrame. We can do this if the join uses the same key names or if the source DataFrames have columns that simply have the same name:
```python
person.join(gradProgramDupe, joinExpr).drop(person["graduate_program"]).select("graduate_program").show()

joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(graduateProgram, joinExpr).drop(graduateProgram["id"]).show()
```
![[Pasted image 20240201123719.png]]
##### Approach 3: Renaming a column before the join
We can avoid this issue altogether if we rename one of our columns before the join:
```python
gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")

joinExpr = person["graduate_program"] == gradProgram3["grad_id"]
person.join(gradProgram3, joinExpr).show()
```
![[Pasted image 20240201123905.png]]
## How Spark Performs Joins
To understand how Spark performs joins, you need to understand the two core resources at play: the **node-to-node communication strategy** and **per node computation strategy**.
#### Communication Strategies
Spark uses two main communication approaches for joins:
- **Shuffle Join:** Involves "all-to-all" communication where data is shuffled across the cluster to different partitions. Used when both tables are large.
- **Broadcast Join:** One table (typically the smaller one) is broadcasted to all worker nodes, reducing network traffic. Used when one table is significantly smaller than the other.
##### Big table-to-big table
When you join a big table to another big table, you end up with a **shuffle join**.
![[Pasted image 20240201124907.png]]

A join strategy where all nodes in the cluster share data with each other based on the join key(s). Used when both tables being joined are large.

**Pros & Cons:**
- **Pro:** Can handle very large datasets.
- **Con:** Expensive due to high network traffic, especially with poor data partitioning.

**Use case:**
- Joining two large datasets where identifying changes over time is needed.
- Example: Joining billions of daily IoT messages based on device ID, message type, and date.

**Key point:**
- Partitioning your data effectively can significantly improve shuffle join performance by reducing network traffic.
##### Big table-to-small table
## Spark Broadcast Join: Summary
This join is ideal when one table (typically much smaller) fits in the memory of a single worker node.

**Benefits:**
- Avoids expensive "all-to-all" communication during the entire join.
- Reduces network traffic by broadcasting the small table to all worker nodes once.
- Enables individual worker nodes to perform joins independently, potentially improving performance.

**Trade-offs:**
- Requires enough memory on a single worker node to hold the entire small table.
- Initial broadcast can be expensive.
- CPU becomes the primary bottleneck after the initial broadcast.

**Key takeaway:**
Broadcast join is a good optimization for joining large tables with small tables, significantly reducing network traffic and potentially improving performance. However, memory constraints and CPU bottlenecks should be considered.
##### Little table–to–little table
When performing joins with small tables, it’s usually best to let Spark decide how to join them. You can always force a broadcast join if you’re noticing strange behavior.
# Chapter 9: Data Sources
Following are Spark’s core data sources:
• CSV 
• JSON 
• Parquet 
• ORC 
• JDBC/ODBC connections 
• Plain-text files

Spark has numerous community-created data sources:
• Cassandra 
• HBase 
• MongoDB 
• AWS Redshift 
• XML 
• And many, many others
## The Structure of the Data Sources
#### Read API Structure
The core structure for reading data is as follows:
```
DataFrameReader.format(...).option("key", "value").schema(...).load()
```
We will use this format to read from all of our data sources. `format` is optional because by default Spark will use the Parquet format. `option` allows you to set key-value configurations to parameterize how you will read data. Lastly, `schema` is optional if the data source provides a schema or if you intend to use schema inference.
#### Basics of Reading Data
The foundation for reading data in Spark is the `DataFrameReader`. We access this through the `SparkSession` via the `read` attribute: 
```
spark.read
```

After we have a DataFrame reader, we specify several values: 
• The **format** 
• The **schema** 
• The **read mode** 
• A series of **options**

Overall layout:
```python
spark.read.format("csv")\ 
	.option("mode", "FAILFAST")\ 
	.option("inferSchema", "true")\ 
	.option("path", "path/to/file(s)")\ 
	.schema(someSchema)\
	.load()
```
There are a variety of ways in which you can set options; for example, you can build a map and pass in your configurations.
##### Read modes
Reading data from an external source naturally entails encountering malformed data, especially when working with only semi-structured data sources. Read modes specify what will happen when Spark does come across malformed records.
![[Pasted image 20240201133635.png]]
The default is **permissive**.
#### Write API Structure
The foundation for writing data is quite similar to that of reading data. Instead of the `DataFrameReader`, we have the `DataFrameWriter`. Because we always need to write out some given data source, we access the `DataFrameWriter` on a `per-DataFrame` basis via the `write` attribute:
```python
dataFrame.write()
```

After we have a `DataFrameWriter`, we specify three values: the **format**, a series of **options**, and the **save mode**. At a minimum, you must supply a path. We will cover the potential for options
```python
dataframe = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/summary2015-1.csv")

dataframe.write.format("csv").option("mode", "OVERWRITE").option("dateFormat", "yyyy-MM-dd").option("path", "path/to/file(s)").save()
```
![[Pasted image 20240201134520.png]]
##### Save modes
Save modes specify what will happen if Spark finds data at the specified location (assuming all else equal).
![[Pasted image 20240201134556.png]]
The default is `errorIfExists`. This means that if Spark finds data at the location to which you’re writing, it will fail the write immediately.
## CSV Files
CSV stands for commma-separated values. This is a common text file format in which each line represents a single record, and commas separate each field within a record.
#### CSV Options
![[Pasted image 20240201134719.png]]
![[Pasted image 20240201134742.png]]
#### Reading CSV Files
```python
spark.read.format("csv")\
 .option("header", "true")\
 .option("mode", "FAILFAST")\
 .option("inferSchema", "true")\
 .load("dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/summary2015-1.csv")
```
#### Writing CSV Files
```python
csvFile = spark.read.format("csv")\
 .option("header", "true")\
 .option("mode", "FAILFAST")\
 .option("inferSchema", "true")\
 .load("dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/summary2015-1.csv")
```

We can take our CSV file and write it out as a TSV file quite easily:
```python
csvFile.write.format("csv").mode("overwrite").option("sep", "\t").save("/tmp/my-tsv-file.tsv")
```
## JSON Files
Spark works with line-delimited JSON files:
- Each line contains a separate JSON object.
- Use `multiLine=True` for entire file as one object.
- Recommended for appending records and better stability.

Advantages of line-delimited JSON:
- Structured data simplifies processing.
- Fewer configuration options due to inherent structure.
- Easier for Spark to infer data types.

Contrast with multiline JSON:
- Single large object per file.
- Less stable for frequent appends.
- Requires more processing and configuration.

**Line-delimited JSON** is preferred for its *efficiency*, *stability*, and *simpler* handling in Spark.
#### JSON Options
![[Pasted image 20240201140901.png]]
![[Pasted image 20240201140938.png]]
#### Reading JSON Files
```python
spark.read.format("json")\
	.option("mode", "FAILFAST")\ 
	.option("inferSchema", "true")\ 
	.load("/data/flight-data/json/2010-summary.json").show(5)
```
#### Writing JSON Files
```python
csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")
```
## Parquet Files
Imagine a spreadsheet where each column is stored separately and compressed. That's essentially Parquet! It's a file format designed for efficient storage and retrieval of large datasets. 

Key Features:
- **Columnar:** Data is stored by column, not row-by-row, allowing faster access to specific columns.
- **Compressed:** Different compression techniques are used to reduce file size.
- **Self-describing:** Includes metadata about the data types and structure.
- **Fast queries:** Optimized for queries that scan only a subset of columns.

Benefits:
- Saves storage space compared to plain text formats like CSV.
- Enables faster data processing, especially for complex queries.
- Widely supported by various big data tools and frameworks.

Use cases:
- Storing and analyzing large datasets in data warehouses and lakes.
- Enabling efficient machine learning and data analytics tasks.

Remember:
- Not as human-readable as plain text formats.
- Optimized for large datasets, not small files.
#### Parquet Options
![[Pasted image 20240201141653.png]]
#### Reading Parquet Files
```python
spark.read.format("parquet").load("/data/flight-data/parquet/2010-summary.parquet").show(5)
```
#### Writing Parquet Files
```python
csvFile.write.format("parquet").mode("overwrite").save("/tmp/my-parquet-file.parquet")
```
## ORC Files
Think of ORCs (Optimized Row Columnar format) as Parquet with some added magic tricks for efficient data handling in Apache Hive and Spark ecosystems.

Key Features:
- **Columnar Storage:** Like *Parquet*, data is stored by column for faster access to specific columns.
- **Advanced Compression:** Uses various techniques like dictionary encoding and run length encoding for even smaller file sizes compared to Parquet. 
- **Self Describing:** Includes metadata about data types and structure. 
- **Predicate Pushdown:** Pushes filtering logic to storage layer for efficient data retrieval.
- **Acid Transactions:** Supports ACID transactions for data consistency in Hive.

Benefits:
- **Smaller Files:** Saves storage space compared to *Parquet* and other formats. 
- **Faster Processing:** Delivers faster query performance due to columnar storage and predicate pushdown. 
- **Hive Integration:** Seamlessly integrated with Hive for efficient data management.
- **Spark Compatibility:** Works well with Spark for data processing and analysis.

Things to Remember:
* **Less Human Readable:** Not as easily readable as plain text formats. 
* **Hive & Spark Ecosystem:** Mainly used in Hive and Spark environments.

Overall:
**ORCs** are a powerful format for efficient data storage and processing in Hive and Spark ecosystems, offering smaller file sizes and faster performance compared to alternatives.
**Think of it as a well organized and compressed data storage solution specifically designed for these big data environments.**
#### Reading Orc Files
```python
spark.read.format("orc").load("/data/flight-data/orc/2010-summary.orc").show(5)
```
#### Writing Orc Files
```python
csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")
```
## SQL Databases
Benefits:
- **Powerful connector:** Access diverse databases using familiar SQL queries.
- **Examples:** MySQL, PostgreSQL, Oracle, SQLite (demonstrated).

Considerations:
- **Authentication:** Secure access to the database system.
- **Connectivity:** Ensure network connection between Spark and database.

SQLite advantage:
- Easy setup, runs locally without distribution limitations.

**Note:** For distributed environments, connect to other database types.

To read and write from these databases, you need to do two things: include the Java Database Connectivity (JDBC) driver for you particular database on the spark class‐ path, and provide the proper JAR for the driver itself. For example, to be able to read and write from PostgreSQL, you might run something like this:
```text
./bin/spark-shell \ 
--driver-class-path postgresql-9.4.1207.jar \ 
--jars postgresql-9.4.1207.jar
```
#### SQL Database Options
![[Pasted image 20240201143126.png]]
![[Pasted image 20240201143138.png]]
#### Reading from SQL Databases
```python
driver = "org.sqlite.JDBC"
path = "dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/my_sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"
```

If this connection succeeds, you’re good to go. Let’s go ahead and read the DataFrame from the SQL table:
```python
dbDataFrame = spark.read.format("jdbc").option("url", url)\ .option("dbtable", tablename).option("driver", driver).load()
```

Let’s perform the same read that we just performed, except using PostgreSQL this time:
```python
pgDF = spark.read.format("jdbc").option("driver", "org.postgresql.Driver").option("url", "jdbc:postgresql://database_server").option("dbtable", "schema.tablename").option("user", "username").option("password", "my-secret-password").load()
```

Get only the distinct locations to verify that we can query it as expected:
```python
dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)
```
![[Pasted image 20240202111721.png]]
#### Query Pushdown
If we specify a filter on our DataFrame, Spark will push that filter down into the database. We can see this in the explain plan under `PushedFilters`:
```python
dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain()
```
![[Pasted image 20240202112349.png]]

To pass an entire query into your SQL:
```python
pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info""" 

dbDataFrame = spark.read.format("jdbc").option("url", url).option("dbtable", pushdownQuery).option("driver", driver).load()
```

![[Pasted image 20240202112923.png]]

##### Reading from databases in parallel
Spark has an underlying algorithm that can read multiple files into one partition, or conversely, read multiple partitions out of one file, depending on the file size and the “splitability” of the file type and compression. The same flexibility that exists with files, also exists with SQL databases except that you must configure it a bit more manually:
```python
dbDataFrame = spark.read.format("jdbc").option("url", url).option("dbtable", tablename).option("driver", driver).option("numPartitions", 10).load()
```

While other APIs offer data partitioning optimizations, this method lets you directly specify predicates within the connection itself. This enables fine-grained control over data location, pushing only relevant subsets (e.g., specific countries) into different Spark partitions for efficient processing. Think of it as filtering and pre-sorting data directly at the database level before bringing it into Spark:
```python
props = {"driver":"org.sqlite.JDBC"} 
predicates = [ "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'", "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"] 

spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show() spark.read.jdbc(url,tablename,predicates=predicates,properties=props).rdd.getNumPartitions()
```
![[Pasted image 20240202114227.png]]

If you specify predicates that are not disjoint, you can end up with lots of duplicate rows. Here’s an example set of predicates that will result in duplicate rows:
```python
props = {"driver":"org.sqlite.JDBC"} 

predicates = [ "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'", "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"] spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count()
```
##### Partitioning based on a sliding window
- Partition data based on numerical ranges for efficient parallel processing.
- Set minimum and maximum bounds for first and last partitions, and Spark distributes data accordingly, without filtering.

```python
colName = "count" 
lowerBound = 0L 
upperBound = 348113L # this is the max count in our database numPartitions = 10
```

This will distribute the intervals equally from low to high:
```python
spark.read.jdbc(url, tablename, column=colName, properties=props, lowerBound=lowerBound, upperBound=upperBound, numPartitions=numPartitions).count()
```
#### Writing to SQL Databases
```python
newPath = "jdbc:sqlite://tmp/my-sqlite.db" csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)
```

Look at the results:
```python
csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)
```

We can append to the table this new table just as easily:
```python
csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)
```

Notice that count increases:
```python
spark.read.jdbc(newPath, tablename, properties=props).count()
```
## Text Files
Spark also allows you to read in plain-text files. Each line in the file becomes a record in the DataFrame. It is then up to you to transform it accordingly.
#### Reading Text Files
```python
spark.read.text("/data/flight-data/csv/2010-summary.csv").selectExpr("split(value, ',') as rows").show()
```
![[Pasted image 20240202120753.png]]
#### Writing Text Files
```python
csvFile.limit(10).select("DEST_COUNTRY_NAME", "count").write.partitionBy("count").text("/tmp/five-csv-files2py.csv")
```
## Advanced I/O Concepts
We can also control specific data layout by controlling two things: **Bucketing** and **Partitioning**.
#### Splittable File Types and Compression
- Certain file formats are **splittable**, enabling Spark to access only necessary parts of a file, improving speed by avoiding reading the entire file.
- Splitting files is particularly beneficial with distributed file systems like *Hadoop Distributed File System (HDFS)*, as it optimizes across multiple blocks.
- However, not all compression schemes support file **splittability**, which can impact performance.
- The choice of file format and compression greatly affects Spark job performance.
- **Parquet with gzip compression** is recommended for optimal performance, offering efficiency and compatibility with Spark while maintaining a good balance between compression ratio and performance.
#### Reading Data in Parallel
- Multiple executors cannot simultaneously read from the same file.
- However, they can read from different files simultaneously.
- When reading from a folder containing multiple files, each file becomes a partition in the DataFrame.
- The available executors read these partitions in parallel, with any remaining executors queuing up behind others.
#### Writing Data in Parallel
The number of files or data written is dependent on the number of partitions the DataFrame has at the time you write out the data. By default, one file is written per partition of the data. This means that although we specify a “file,” it’s actually a number of files within a folder, with the name of the specified file, with one file per each partition that is written.

For example, the following code
```python
csvFile.repartition(5).write.format("csv").save("/tmp/multiple.csv")
```
will end up with five files inside of that folder.
![[Pasted image 20240202122038.png]]
![[Pasted image 20240202122049.png]]
#### Partitioning
**Partitioning** allows you to control data storage by encoding a column as a folder, enabling selective data retrieval. This means you can read only relevant data, bypassing unnecessary scans of the complete dataset.
```python
csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME").save("/tmp/partitioned-files.parquet")
```

Upon writing, you get a list of folders in your Parquet “file”:
![[Pasted image 20240202122340.png]]
Each of these will contain Parquet files that contain that data where the previous predicate was true:
![[Pasted image 20240202122401.png]]
#### Bucketing
- Bucketing is an alternative file organization method to partitioning.
- It allows control over which data is written to each file.
- Data with the same bucket ID is grouped together into one physical partition.
- Prepartitioning data based on expected usage can avoid costly shuffles during operations like joining or aggregating.
- Bucketing creates a specific number of files and organizes data into these "buckets," offering efficiency compared to partitioning, which may create numerous directories.

```scala
val numberBuckets = 10 val columnToBucketBy = "count" 
csvFile.write.format("parquet").mode("overwrite").bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")
```
![[Pasted image 20240202122620.png]]
## Managing File Size
- Managing file sizes is crucial, especially for efficient data reading.
- Handling numerous small files incurs significant metadata overhead, known as the "small file problem," which affects Spark's performance and is problematic for file systems like HDFS.
- Conversely, excessively large files can lead to inefficiency when reading entire blocks of data for only a few required rows.
- Spark 2.2 introduced a method to automatically control file sizes.
- The `maxRecordsPerFile` option allows specifying the maximum number of records per file.
- Setting this option (e.g., `df.write.option("maxRecordsPerFile", 5000)`) ensures each file contains at most the specified number of records, aiding in better file size management.
# Chapter 10: Spark SQL
## The Hive Metastore
- To connect to the Hive metastore, specific properties are required.
- Set `spark.sql.hive.metastore.version` to correspond to the Hive metastore version being accessed, typically set to 1.2.1 by default.
- Adjust `spark.sql.hive.metastore.jars` if customization of the HiveMetastoreClient initialization is needed, allowing specification of Maven repositories or a classpath.
- Proper class prefixes might be necessary for communication with different databases storing the Hive metastore; these are set as shared prefixes shared between Spark and Hive (`spark.sql.hive.metastore.sharedPrefixes`).
## Spark SQL CLI
To start the Spark SQL CLI, run the following in the Spark directory:
```
./bin/spark-sql
```

You configure Hive by placing your `hive-site.xml`, `core-site.xml`, and `hdfs-site.xml` files in conf/. For a complete list of all available options, you can run:
```
./bin/spark-sql -- help
```
## Spark's Programming SQL Interface
You can do this via the method `sql` on the `SparkSession` object.
```python
spark.sql("SELECT 1 + 1").show()
```
![[Pasted image 20240202124139.png]]

You can express multiline queries quite simply by passing a multiline string into the function:
```python
spark.sql("""SELECT user_id, department, first_name FROM professors WHERE department IN (SELECT name FROM department WHERE created_date >= '2016-01-01')""")
```

Even more powerful, you can completely interoperate between SQL and DataFrames, as you see fit. For instance, you can create a DataFrame, manipulate it with SQL, and then manipulate it again as a DataFrame:
```python
spark.read.json("dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/2015_summary-1.json").createOrReplaceTempView("some_sql_view") # DF => SQL 

spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""").where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10").count()
```
![[Pasted image 20240202124834.png]]
## SparkSQL Thrift JDBC/ODBC Server
- Spark offers a JDBC interface for executing Spark SQL queries, allowing connections from either local or remote programs to the Spark driver.
- This interface facilitates scenarios like connecting business intelligence software (e.g., Tableau) to Spark for data analysis.
- The Thrift JDBC/ODBC server in Spark corresponds to HiveServer2 in Hive 1.2.1, enabling compatibility with existing tools and scripts.
- To start the JDBC/ODBC server, run `./sbin/start-thriftserver.sh` in the Spark directory, which accepts various command-line options and listens on `localhost:10000` by default.
- Configuration options for the Thrift Server can be specified through environmental variables or system properties.
- Testing the JDBC/ODBC connection can be done using the `beeline` script provided with Spark or Hive 1.2.1, connecting to `jdbc:hive2://localhost:10000`.
## Catalog
- The highest level abstraction in Spark SQL is the Catalog, which stores metadata about tables, databases, functions, and views.
- It resides in the `org.apache.spark.sql.catalog.Catalog` package and offers functions for tasks like listing tables, databases, and functions.
- The Catalog is user-friendly and provides a programmatic interface to Spark SQL for managing metadata.
- While code samples are omitted, users need to wrap relevant code in a `spark.sql` function call when using the programmatic interface to execute SQL commands.

## Tables
#### Creating Tables
Here’s a simple way to read in the flight data we worked with in previous chapters:
```SQL
CREATE TABLE flights (
	DEST_COUNTRY_NAME STRNG, ORIGIN_COUNTRY_NAME STRING, count LONG)
USING JSON
OPTIONS (path 'dbfs:/FileStore/shared_uploads/ayaan2911@aol.com/summary2015-2.csv')
)
```
![[Pasted image 20240202140319.png]]

![[Pasted image 20240202135824.png]]

You can also add comments to certain columns in a table, which can help other developers understand the data in the tables:
```sql
CREATE TABLE flights_csv (
 DEST_COUNTRY_NAME STRING,
 ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
 count LONG)

USING csv
OPTIONS (header true, path '/data/flight-data/csv/2015-summary.csv')
```

It is possible to create a table from a query as well. The following program populates the schema and data from the flights table:
```sql
CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights
```
![[Pasted image 20240202141210.png]]

In addition, you can specify to create a table only if it does not currently exist:
```sql
CREATE TABLE IF NOT EXISTS flights_from_select AS SELECT * FROM flights
```

You can control the layout of the data by writing out a partitioned dataset:
```sql
CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME) AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights 
LIMIT 5
```
#### Creating External Tables
External tables in database management systems refer to tables where the data files are stored outside of the database's directory. Instead of managing the data files internally, the database simply references them.
You create this table by using the `CREATE EXTERNAL TABLE` statement:
```sql
CREATE EXTERNAL TABLE hive_flights ( DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/flight-data-hive/'
```
or,
using the `SELECT` statement:
```sql
CREATE EXTERNAL TABLE hive_flights_2
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/flight-data-hive/' AS SELECT * FROM flights
```
#### Inserting into Tables
```sql
INSERT INTO flights_from_select
SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights
LIMIT 20
```

You can optionally provide a partition specification if you want to write only into a certain partition:
```sql
INSERT INTO partitioned_flights PARTITION (DEST_COUNTRY_NAME="UNITED STATES")
SELECT count, ORIGIN_COUNTRY_NAME FROM flights
WHERE DEST_COUNTRY_NAME='UNITED STATES'
LIMIT 12
```
#### Describing Table Metadata
```sql
DESCRIBE TABLE flights_csv
```
![[Pasted image 20240202145104.png]]

You can also see the partitioning scheme for the data by using the following (note, however, that this works only on partitioned tables):
```sql
SHOW PARTITIONS partitioned_flights
```
![[Pasted image 20240202145144.png]]
#### Refreshing Table Metadata
`REFRESH TABLE` refreshes all cached entries (essentially, files) associated with the table. If the table were previously cached, it would be cached lazily the next time it is scanned:
```sql
REFRESH table partitioned_flights
```

`REPAIR TABLE`, which refreshes the partitions maintained in the catalog for that given table. This command’s focus is on collecting new partition information:
```sql
MSCK REPAIR TABLE partitioned_flights
```
#### Dropping Tables
```sql
DROP TABLE flights_csv
```

If you try to drop a table that does not exist, you will receive an error. To only delete a table if it already exists, use `DROP TABLE IF EXISTS`:
```sql
DROP TABLE IF EXISTS flights_csv
```
##### Dropping unmanaged tables
If you are dropping an unmanaged table (e.g., *hive_flights*), no data will be removed but you will no longer be able to refer to this data by the table name.
#### Caching Tables
For **Caching**:
```sql
CACHE TABLE flights
```
For **Uncaching**:
```sql
UNCACHE TABLE flights
```
## Views
A view in a database is a virtual table that is based on the result set of a SELECT query. Unlike physical tables, which store data persistently, views do not contain data themselves; instead, they represent a stored query that can be queried like a table.
#### Creating Views
```sql
CREATE VIEW just_usa_view AS SELECT * FROM flights 
WHERE dest_country_name = 'United States'
```

For temporary views:
```sql
CREATE TEMP VIEW just_usa_view_temp AS SELECT * FROM flights 
WHERE dest_country_name = 'United States'
```

For global temporary views:
```sql
CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS SELECT * FROM flights 
WHERE dest_country_name = 'United States' 
```

You can also specify that you would like to overwite a view if one already exists by using the keywords shown in the sample that follows. We can overwrite both temp views and regular views:
```sql
CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS SELECT * FROM flights
WHERE dest_country_name = 'United States'
```
Now you can query this view just as if it were another table:
```sql
SELECT * FROM just_usa_view_temp
```

To explain a view:
```sql
EXPLAIN SELECT * FROM just_usa_view
```
#### Dropping Views
```sql
DROP VIEW IF EXISTS just_usa_view
```
## Databases
You can see all databases by using the following command:
```sql
SHOW DATABASES
```
#### Creating Databases
```sql
CREATE DATABASE some_db
```
#### Setting the Database
You might want to set a database to perform a certain query:
```sql
USE some_db
```

After you set this database, all queries will try to resolve table names to this database.
Queries that were working just fine might now fail or yield different results because you are in a different database:
```sql
SHOW tables
SELECT * FROM flights --fails with table/view not found
```

To fix this, use the correct prefix:
```sql
SELECT * FROM default.flights
```

To see what database you're currently using by running the following command:
```sql
SELECT current_database()
```

To switch back to default database:
```sql
USE default
```
#### Dropping Databases
```sql
DROP DATABASE IF EXISTS some_db
```
## SELECT Statements
![[Pasted image 20240202155848.png]]
![[Pasted image 20240202155858.png]]
## case...when...then Statements
```sql
SELECT CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1 WHEN DEST_COUNTRY_NAME = 'Egypt' THEN 0 ELSE -1 END FROM partitioned_flights
```
## Advanced Topics
#### Complex Types
There are three core complex types in Spark SQL: **structs**, **lists** and **maps**.
##### structs
To create one, you simply need to wrap a set of columns (or expressions) in parentheses:
```sql
CREATE VIEW IF NOT EXISTS nested_data AS SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights
```

You can even query individual columns within a struct—all you need to do is use dot syntax:
```sql
SELECT country.DEST_COUNTRY_NAME, count FROM nested_data
```

If you like, you can also select all the subvalues from a struct by using the struct’s name and select all of the subcolumns:
```sql
SELECT country.*, count FROM nested_data
```
##### Lists
```sql
SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts, collect_set(ORIGIN_COUNTRY_NAME) as origin_set FROM flights GROUP BY DEST_COUNTRY_NAME
```
![[Pasted image 20240202162327.png]]

You can, however, also create an array manually within a column, as shown here:
```sql
SELECT DEST_COUNTRY_NAME, ARRAY(1, 2, 3) FROM flights
```
![[Pasted image 20240202162259.png]]

You can also query lists by position by using a Python-like array query syntax:
```sql
SELECT DEST_COUNTRY_NAME as new_name, collect_list(count)[0] FROM flights GROUP BY DEST_COUNTRY_NAME
```

You can also do things like convert an array back into rows. You do this by using the `explode` function:
```sql
CREATE OR REPLACE TEMP VIEW flights_agg AS SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts FROM flights
GROUP BY DEST_COUNTRY_NAME
```
## Functions
To see a list of functions in Spark SQL, you use the `SHOW FUNCTIONS` statement:
```sql
SHOW FUNCTIONS
```
![[Pasted image 20240202163036.png]]

You can also more specifically indicate whether you would like to see the system functions (i.e., those built into Spark) as well as user functions:
```sql
SHOW SYSTEM FUNCTIONS
```
![[Pasted image 20240202163115.png]]

For user functions:
```sql
SHOW USER FUNCTIONS
```
![[Pasted image 20240202163153.png]]

You can filter all `SHOW` commands by passing a string with wildcard (`*`) characters. Here, we can see all functions that begin with “s”:
```sql
SHOW FUNCTIONS "s*";
```
![[Pasted image 20240202163536.png]]

You can also use the `LIKE` keyword.
#### User-Defined Functions
```python
def power3(number:Double):Double = number * number * number spark.udf.register("power3", power3(_:Double):Double) 
```
```sql
SELECT count, power3(count) FROM flights
```
## Subqueries
#### Uncorrelated
```sql
SELECT * FROM flights 
WHERE origin_country_name IN (SELECT dest_country_name FROM flights GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5)
```
#### Correlated
```sql
SELECT * FROM flights f1 
WHERE EXISTS (SELECT 1 FROM flights f2 WHERE f1.dest_country_name = f2.origin_country_name) AND EXISTS (SELECT 1 FROM flights f2 WHERE f2.dest_country_name = f1.origin_country_name)
```
#### Uncorrelated Scalar
```sql
SELECT *, (SELECT max(count) FROM flights) AS maximum FROM flights
```
## Configurations
![[Pasted image 20240202163905.png]]
![[Pasted image 20240202163935.png]]
#### Setting these config values
```sql
SET spark.sql.shuffle.partitions=20
```
# Chapter 11: Datasets

- Datasets are used when operations cannot be expressed using DataFrame manipulations or when type-safety is desired, even at the cost of performance.
- They are suitable for encoding large sets of business logic into specific functions, providing an appropriate use case for Datasets.
- The Dataset API ensures type-safety, as operations that are not valid for their types will fail at compilation time rather than runtime, contributing to correctness and bulletproof code.
- Datasets can elegantly handle and organize data, although they do not protect against malformed data.
- Another scenario for using Datasets is when reusing transformations between single-node and Spark workloads, leveraging the similarity between Spark APIs and Scala Sequence Types.
- Defining data and transformations as accepting case classes allows for easy reuse of Datasets for both distributed and local workloads.
- Using DataFrames and Datasets in tandem is a popular approach, allowing for manual trade-offs between performance and type safety based on workload requirements.
- This approach might involve collecting data to the driver for manipulation using single-node libraries, or performing per-row parsing before filtering and further manipulation in Spark SQL, depending on the transformation workflow.
## Creating Datasets
#### In Scala: Case Classes
To create Datasets in Scala, you define a Scala case class. A ***case class*** is a regular class that has the following characteristics: 
• Immutable 
• Decomposable through pattern matching 
• Allows for comparison based on structure instead of reference 
• Easy to use and manipulate

To begin creating a Dataset, let’s define a ***case class*** for one of our datasets:
```scala
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
```

Now that we defined a case class, this will represent a single record in our dataset. More succintly, we now have a Dataset of Flights. This doesn’t define any methods for us, simply the schema. When we read in our data, we’ll get a DataFrame. However, we simply use the as method to cast it to our specified row type:
```python
flightsDF = spark.read.parquet("/data/flight-data/parquet/2010-summary.parquet/")
flights = flightsDF.as[Flight]
```
## Actions
```python
flights.show(2)
```
