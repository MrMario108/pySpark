from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def df_size_in_mb(df):
    size_bytes = df.rdd.map(lambda row: sys.getsizeof(row)).sum()
    size_mb = size_bytes / (1024 * 1024)
    return size_mb


def getGrocerySales() -> DataFrame:
    spark: SparkSession
    spark = SparkSession.builder.master("local[1]").appName('GrocerySales').getOrCreate()

    df_categories = spark.read.options(header=True, decimal=',').csv('src/grocerySales/categories.csv')
    df_cities = spark.read.options(header=True, decimal=',').csv('src/grocerySales/cities.csv')
    df_countries = spark.read.options(header=True, decimal=',').csv('src/grocerySales/countries.csv')
    df_customers = spark.read.options(header=True, decimal=',').csv('src/grocerySales/customers.csv')
    df_employees = spark.read.options(header=True, decimal=',').csv('src/grocerySales/employees.csv').withColumnRenamed("FirstName", "EmployeFirstName").withColumnRenamed("LastName", "EmployeLastName")
    df_products = spark.read.options(header=True, decimal=',').csv('src/grocerySales/products.csv')
    df_sales = spark.read.options(header=True, decimal=',').csv('src/grocerySales/sales.csv')
    
    return df_categories, df_cities, df_countries, df_customers, df_employees, df_products, df_sales
    

def getDFGrocerySalesFull(df_categories, df_cities, df_countries, df_customers, df_employees, df_products, df_sales: DataFrame):
    df_grocery_sales_full: DataFrame

    df_grocery_sales_full = (
        df_sales
        .join(df_employees, df_sales.SalesPersonID == df_employees.EmployeeID, "left")
        .join(df_customers, "CustomerID", "left")
        .withColumnRenamed("FirstName", "CustomerFirstName")
        .withColumnRenamed("LastName", "CustomerLastName")
        .join(df_products, "ProductID", "left")
        .join(df_categories, "CategoryID", "left")
        .join(df_cities, "CityID", "left")
        .join(df_countries, "CountryID", "left")
        )
    
    return df_grocery_sales_full


def getDFGrocerySalesCustomers(df_cities, df_countries, df_customers: DataFrame) -> DataFrame:
    df_grocery_customers: DataFrame

    df_grocery_customers = (
        df_customers
        .join(df_cities, "CityID", "left")
        .join(df_countries, "CountryID", "left")
        )
    
    return df_grocery_customers

def sqlSpark():
    return SparkSession.builder.master("local[1]").appName('SqlGrocerySales').getOrCreate()

def sqlGroceryCustomers(df: DataFrame, spark: SparkSession):
    df.createOrReplaceTempView("tableCustomers")
    df.printSchema()
    spark.sql("SELECT * from tableCustomers").show()
    # Wyświetli zgrupowane unikalne nazwy miast
        # spark.sql("SELECT CityName from tableCustomers GROUP BY CityName").show()
    # Wyświetli unikalne nazwy miast
        # spark.sql("SELECT distinct CityName from tableCustomers ORDER BY CityName").show(100)
    # Wyświetli unikalne kombinacje nazwy miast i imion
    spark.sql("SELECT distinct CityName, FirstName from tableCustomers ORDER BY FirstName").show(100)
    # sprawdzamy poprzednie wywołanie
    spark.sql("SELECT * from tableCustomers WHERE FirstName='Abel' and CityName='San Francisco'").show(100)
    # sprawdzamy poprzednie wywołanie
    spark.sql("SELECT CityName, count(FirstName) from tableCustomers where FirstName='Abel' GROUP BY CityName ORDER BY CityName desc").show(100)



def sqlGrocerySales(df: DataFrame, spark):
    df.createOrReplaceTempView("tableA")
    
    # View schema:
    # |CountryID|CityID|CategoryID|ProductID|SalesID|SalesPersonID|CustomerID|Quantity|Discount|TotalPrice|
    # SalesDate|   TransactionNumber|EmployeeID|EmployeFirstName|MiddleInitial|EmployeLastName|           BirthDate|Gender|
    # HireDate|CustomerID|CustomerFirstName|MiddleInitial|CustomerLastName|CityID|             Address|         ProductName|  Price| Class|
    # ModifyDate|Resistant|IsAllergic|VitalityDays|CategoryName|   CityName|Zipcode|  CountryName|CountryCode|
    
    # Pokaż całość
    spark.sql("SELECT * from tableA").show()

    spark.sql("SELECT CustomerID, SUM(Price * Quantity) AS OrdersValue from tableA GROUP BY CustomerID").show()

    # Pokaż wszystko + sumę w każdym wierszu // nie wszystkie bazy danych obsługują  -- błede dane pokazuje
    spark.sql("SELECT *, SUM(Price * Quantity) OVER () AS OrdersValue FROM tableA;").show()

    # Pokaż wszystko + sumę

    #  spark.sql("SELECT a.*, totals.OrdersValue FROM tableA a JOIN (SELECT OrderID, SUM(Price * Quantity) AS OrdersValue FROM tableA GROUP BY OrderID) totals ON a.OrderID = totals.OrderID;").show()

def example_0():
    spark: SparkSession
    data_frame: DataFrame
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

    # Schemat: Brand,Model,Year,Engine_Size,Fuel_Type,Transmission,Mileage,Doors,Owner_Count,Price
    # Przykład wiersza: Kia,Rio,2020,4.2,Diesel,Manual,289944,3,5,8501
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

    # Wyszukaj najnowyszy samochód według daty
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df = data_frame.withColumn('date',to_timestamp("Year", 'yyyy'))
    df.select(max("date")).show()

    # Posortuj według daty wyswietl 100 wierszy i obetnij zawartość kolum do 2 znaków
    data_frame.orderBy(desc('Year')).show(100,2)
    # data_frame.agg()

    data = [("Volkswagen","Golf","2009","4.5","Hybrid","Manual",42795,4,3,11444)]

    # Dodaj kolumnę ze stałą wartością
    data_frame.withColumn("from_year", lit("1900")).show(5,0)

    # data_frame.write.option("header",True).mode('append').csv("src/car_price_dataset.csv")
    # data_frame_test = createDataFrame

    df_freq = data_frame.groupBy('Brand').agg(avg('Mileage').alias('Mileage_For_Brand')).show()
    df_freq = data_frame.groupBy('Brand').avg('Mileage').show()

def example_1():
    df_categories, df_cities, df_countries, df_customers, df_employees, df_products, df_sales = getGrocerySales()
    df_full = getDFGrocerySalesFull(df_categories, df_cities, df_countries, df_customers, df_employees, df_products, df_sales)
    df_customers = getDFGrocerySalesCustomers(df_cities, df_countries, df_customers)

#    df_grocery_sales_cleared = df.select("SalesID", "Quantity", "TotalPrice", "SalesDate", "TransactionNumber", "EmployeFirstName", "EmployeLastName","CustomerFirstName", "CustomerLastName", "ProductName",  "Price", "Class","CategoryName", "CityName", "CountryName")
    
    # zapis do jednego pliku
    # df_grocery_sales_cleared.coalesce(1).write.mode("overwrite").csv("src/grocery_sales_cleared.csv", header=True)
    
    # zapis z podziałem na wiele plików
    # df_grocery_sales_cleared.write.mode("overwrite").csv("src/grocery_sales_cleared.csv", header=True)
    sqlGrocerySales(df_full, sqlSpark())

    sqlGroceryCustomers(df_customers, sqlSpark())


    # print(f"Rozmiar referencji DataFrame: {sys.getsizeof(df):.2f} MB")
    # print(f"Rozmiar bazy danych: {df_size_in_mb(df):.2f} MB")

example_1()