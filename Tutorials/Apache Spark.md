This is done on **Google Collab** so the setup process is of Google Collab only.

## 1. Setting up Spark

```python
!sudo apt update

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

!wget -q https://dlcdn.apache.org/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz

!tar xf spark-3.4.2-bin-hadoop3.tgz

!pip install -q findspark

!pip install pyspark
```
![[Pasted image 20231227194334.png]]

```python
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.4.2-bin-hadoop3"
```

```python
import findspark

findspark.init()
findspark.find()
```

```python
from pyspark.sql import DataFrame, SparkSession
from typing import List
import pyspark.sql.types as T
import pyspark.sql.functions as F
```

```python
spark = SparkSession \
       .builder \
       .appName("Our First Spark example") \
       .getOrCreate()
```

```python
spark
```
![[Pasted image 20231227194506.png]]

## 2. Loading Data 

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import split, count, when, isnan, col, regexp_replace
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
```

```python
# Define a schema
schema = StructType([StructField('mpg', FloatType(), nullable = True),
                     StructField('cylinders', IntegerType(), nullable = True),
                     StructField('displacement', FloatType(), nullable = True),
                     StructField('horsepower', StringType(), nullable = True),
                     StructField('weight', IntegerType(), nullable = True),
                     StructField('acceleration', FloatType(), nullable = True),
                     StructField('model year', IntegerType(), nullable = True),
                     StructField('origin', IntegerType(), nullable = True),
                     StructField('car name', StringType(), nullable = True)])

file_path = '/content/auto-mpg.csv'
```

```python
df = spark.read.csv(file_path, header = True, inferSchema = True, nanValue = '?')
df.show(5)
```
![[Pasted image 20231227195323.png]]

```python
# Check for missing values
def check_missing(dataframe):

  return dataframe.select([count(when(isnan(c) | col(c).isNull(), c)). \
                           alias(c) for c in dataframe.columns]).show()

check_missing(df)
```
![[Pasted image 20231227195311.png]]

```python
# Handling Missing Values
df = df.na.drop()

df = df.withColumn("horsepower", df["horsepower"].cast(IntegerType()))   # Convert Horsepower from str to int

df.show(5)
```
![[Pasted image 20231227195430.png]]

## 3. PySpark DataFrame Basics

```python
df.columns
```
![[Pasted image 20231227195557.png]]

```python
df.printSchema()
```
![[Pasted image 20231227195622.png]]

```python
# Renaming Columns
df = df.withColumnRenamed('model year', 'model_year')
df = df.withColumnRenamed('car name', 'car_name')

df.show(3)
```
![[Pasted image 20231227195726.png]]

```python
# Get info from first 4 rows
for car in df.head(4):
    print(car, '\n')
```
![[Pasted image 20231227195754.png]]

```python
# statistical summary of dataframe
df.describe().show()
```
![[Pasted image 20231227195831.png]]

```python
# describe with specific variables
df.describe(['mpg', 'horsepower']).show()
```
![[Pasted image 20231227195850.png]]

```python
# Describe with numerical columns
def get_num_cols(dataframe):
  
  num_cols = [col for col in dataframe.columns if dataframe.select(col). \
              dtypes[0][1] in ['double', 'int']]

  return num_cols
  
num_cols = get_num_cols(df)

df.describe(num_cols).show()
```
![[Pasted image 20231227200001.png]]

## 4. Spark DataFrame Basic Options
#### Filtering and Sorting

```python
# Get the cars with mpg more than 23
df.filter(df['mpg'] > 23).show(5)
```
![[Pasted image 20231227200058.png]]

```python
# Multiple Conditions
df.filter((df['horsepower'] > 80) &
          (df['weight'] > 2000)).select('car_name').show(5)
```
![[Pasted image 20231227200123.png]]

```python
# Sorting
df.filter((df['mpg'] > 25) & (df['origin'] == 2)). \
orderBy('mpg', ascending= False).show(5)
```
![[Pasted image 20231227200144.png]]

```python
# Get the cars with 'volkswagen' in their names, and sort them by model year and horsepower
df.filter(df['car_name'].contains('volkswagen')). \
orderBy(['model_year', 'horsepower'], ascending=[False, False]).show(5)
```
![[Pasted image 20231227200206.png]]

#### Filtering with SQL

```python
# Get the cars with Toyota in their names
df.filter("car_name like '%toyota%'").show(5)
```
![[Pasted image 20231227201215.png]]

```python
df.filter('mpg > 22').show(5)
```
![[Pasted image 20231227201235.png]]

```python
#Multiple Conditions
df.filter('mpg > 22 and acceleration < 15').show(5)
```
![[Pasted image 20231227201255.png]]

```python
df.filter('horsepower == 88 and weight between 2600 and 3000') \
.select(['horsepower', 'weight', 'car_name']).show()
```
![[Pasted image 20231227201324.png]]

## 5. GroupBy and Aggregate Operations

```python
# Brands
df.createOrReplaceTempView('auto_mpg')
df = df.withColumn('brand', split(df['car_name'], ' ').getItem(0)).drop('car_name')
  
# Replacing Misspelled Brands
auto_misspelled = {'chevroelt': 'chevrolet',
                   'chevy': 'chevrolet',
                   'vokswagen': 'volkswagen',
                   'vw': 'volkswagen',
                   'hi': 'harvester',
                   'maxda': 'mazda',
                   'toyouta': 'toyota',
                   'mercedes-benz': 'mercedes'}

for key in auto_misspelled.keys():
  
  df = df.withColumn('brand', regexp_replace('brand', key, auto_misspelled[key]))
  
df.show(5)
```
![[Pasted image 20231227201436.png]]

```python
# Average acceleration by car brands
df.groupBy('brand').agg({'acceleration': 'mean'}).show(5)
```
![[Pasted image 20231227201515.png]]

```python
# Max MPG by car brands
df.groupBy('brand').agg({'mpg': 'max'}).show(5)
```
![[Pasted image 20231227201537.png]]

```python
spark.stop()
```