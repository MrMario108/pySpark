from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def initSession() -> SparkSession:
    spark = SparkSession.builder \
        .appName("SQLite_Spark") \
        .config("spark.jars", "src/jars/sqlite-jdbc-3.49.1.0.jar") \
        .getOrCreate()

    return spark

def loadTables(spark: SparkSession) -> DataFrame:
    # 📂 Ścieżka do bazy SQLite
    database_path = "src/airlines/newTravels.sqlite"
#    database_path = "src/s_p.sqlite"


    # 🔗 Definiujemy parametry JDBC
    jdbc_url = f"jdbc:sqlite:{database_path}"

    table_names = ["aircrafts_data", "bookings", "seats", "tickets", "boarding_passes", "ticket_flights", "airports_data", "flights"]

    dfs = {table: spark.read
                      .format("jdbc")
                      .option("url", jdbc_url)
                      .option("dbtable", table)
                      .option("driver", "org.sqlite.JDBC")
                         .load()
           for table in table_names}

    dfs["seats"].printSchema()
    dfs["tickets"].printSchema()
    dfs["boarding_passes"].printSchema()
    dfs["ticket_flights"].printSchema()
    dfs["airports_data"].printSchema()
    dfs["flights"].printSchema()
    
    #df_fixed.createOrReplaceTempView("seatsSQLView")
    #spark.sql("SELECT * FROM seatsSQLView;").show()

    dfs["seats"].show()


def main():
    loadTables(initSession())

main()
