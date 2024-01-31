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

Another common task is to compute summary statistics for a column or set of col‐  
umns. We can use the `describe` method to achieve exactly this:
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