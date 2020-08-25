spark2-shell --master yarn --conf spark.ui.port=0 --num-executors 6 --executor-cores 2 --executor-memory 2G

//Load
scala> val  data = spark.read.option("sep",",").option("header","true").option("inferSchema","true").csv("/public/crime/csv/crime_data.csv")
data: org.apache.spark.sql.DataFrame = [ID: int, Case Number: string ... 20 more fields

scala> data.printSchema
root
 |-- ID: integer (nullable = true)
 |-- Case Number: string (nullable = true)
 |-- Date: string (nullable = true)
 |-- Block: string (nullable = true)
 |-- IUCR: string (nullable = true)
 |-- Primary Type: string (nullable = true)
 |-- Description: string (nullable = true)
 |-- Location Description: string (nullable = true)
 |-- Arrest: boolean (nullable = true)
 |-- Domestic: boolean (nullable = true)
 |-- Beat: integer (nullable = true)
 |-- District: integer (nullable = true)
 |-- Ward: integer (nullable = true)
 |-- Community Area: integer (nullable = true)
 |-- FBI Code: string (nullable = true)
 |-- X Coordinate: integer (nullable = true)
 |-- Y Coordinate: integer (nullable = true)
 |-- Year: integer (nullable = true)
 |-- Updated On: string (nullable = true)
 |-- Latitude: double (nullable = true)
 |-- Longitude: double (nullable = true)
 |-- Location: string (nullable = true)

data.first
res1: org.apache.spark.sql.Row = [5679862,HN487108,07/24/2007 10:11:00 PM,054XX S ABERDEEN ST,1320,CRIMINAL DAMAGE,TO VEHICLE,STREET,false,false,934,9,16,61,14,1169912,1868555,2007,04/15/2016 08:55:02 AM,41.794811309,-87.652466989,(41.794811309, -87.652466989

data.count
res2: Long = 6397406

val finalData = data.withColumn("Date",to_date(concat(substring($"Date",7,4),lit("-"),substring($"Date",4,2),lit("-"),substring($"Date",1,2)),"yyyy-dd-MM"))

val neededData = finalData.select(date_format($"Date","yyyyMM").as("Date"),$"Primary Type")

val result = neededData.groupBy("Date","Primary Type").agg(count(lit(1)).alias("Count")).orderBy($"Date",$"Count".desc).select($"Date",$"Count",$"Primary Type")

result.coalesce(4).write.format("csv").option("header","true").save("crime/crime_per_month.csv")

//With Compression and tab delimited.
result.coalesce(4).write.format("csv").option("header","true").option("sep","\t").option("compression","gzip").save("crime/compressed/crime_per_month.csv")
