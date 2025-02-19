from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[1]").appName('SparkExample').getOrCreate()

# print(spark)
#rdd=spark.sparkContext.parallelize([1,2,3,56])
# rdd=spark.sparkContext.textFile("test.txt")

#print("RDD count: "+str(rdd.count()))

# data = [('James','', 'Smith', '1991-04-01', 'M', 3000), ('Michael','', 'Smith', '1994-04-01', 'M', 3000), ('Janet','', 'Smith', '1999-04-01', 'F', 1000), ('Ola','', 'S', '2991-04-01', 'F', -1),]
# 
# columns = ["first_name", "middle_name", "dob", "gender", "salary"]
# data_frame = spark.createDataFrame(data=data, schema=columns)
# 
# data_frame.printSchema()

# data_frame = spark.read.option("header",True).csv("src/car_price_dataset.csv")

data_frame = spark.read.options(header=True,delimiter=',').csv("src/car_price_dataset.csv")
# data_frame.printSchema()
# data_frame.show(truncate=False)

# Analysis

# Count the numbers of rows
data_frame.count()

# How many unique customers are present in the DataFrame?
df = data_frame.select('Brand').distinct().count()
print(df)

# How many unique customers are present in the DataFrame?
df = data_frame.select('Brand').distinct().show()

# Pogrupuj w marki, policz Modele i posortuj malejąco
data_frame.groupBy('Brand').agg(countDistinct('Model').alias('model_count')).orderBy(desc('model_count')).show()





data = [("Volkswagen","Golf","2009","4.5","Hybrid","Manual",42795,4,3,11444)]



# data_frame.write.option("header",True).mode('append').csv("src/car_price_dataset.csv")
# data_frame_test = createDataFrame