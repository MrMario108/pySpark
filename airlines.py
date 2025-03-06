from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def initSession() -> SparkSession:
    spark = SparkSession.builder \
        .appName("SQLite_Spark") \
        .config("spark.jars", "src/jars/sqlite-jdbc-3.49.1.0.jar") \
        .getOrCreate()

    return spark

def loadTables(spark: SparkSession) -> Dict:
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

    return dfs

def printSchemas(dfs: Dict) -> None:
    table_df: DataFrame

    for key, table_df in dfs.items():
        print(f"Table name: {key}")
        table_df.printSchema()

def makeFullRelations(dfs: Dict):
    """Złączenie wszystkiego w przypadku tej bazy danych w jeden DataFrame wydaje się nie logiczne.
        Można tą bazę inaczej zaprojektować
        Funkcja niedokończona bo nie ma takiej potrzeby"""

    dfs_all_relations: DataFrame
    aircrafts_data: DataFrame
    flights: DataFrame
    boarding_passes: DataFrame
    aircrafts_data = dfs["aircrafts_data"]
    seats = dfs["seats"]
    flights = dfs["flights"]
    boarding_passes = dfs["boarding_passes"]


    dfs_all_relations = (
        flights
        .join(aircrafts_data, flights.aircraft_code == aircrafts_data.aircraft_code, "left")
        .join(seats, aircrafts_data.aircraft_code == seats.aircraft_code, "left")
        .join(boarding_passes, flights.flight_id == boarding_passes.flight_id, "left")
    )
    dfs_all_relations.show()

    return dfs_all_relations

def seatsInfo(df: DataFrame, spark: SparkSession):

    df.createOrReplaceTempView("seatsAirplane")
    # pokaż wszystko
    spark.sql("SELECT * FROM seatsAirplane;").show()
    
    # policz ile jest samolotów
    spark.sql("SELECT COUNT(DISTINCT aircraft_code) FROM seatsAirplane;").show()

    # jakie modele samolotów występują
    spark.sql("SELECT DISTINCT aircraft_code FROM seatsAirplane;").show()
    
    # pokaż który samolot ma najwięcej miejsc
    spark.sql("SELECT aircraft_code, COUNT(aircraft_code) as seats_number FROM seatsAirplane GROUP BY aircraft_code ORDER BY seats_number;").show()

    # jakiej klasy są miejsca w danym samolocie
    spark.sql("SELECT DISTINCT fare_conditions FROM seatsAirplane WHERE aircraft_code = '773';").show()
    spark.sql("SELECT DISTINCT fare_conditions FROM seatsAirplane WHERE aircraft_code = 'CN1';").show()

    # ile jest miejsc klasy Economy
    spark.sql("SELECT aircraft_code, COUNT(fare_conditions) FROM seatsAirplane WHERE aircraft_code = '773' and fare_conditions = 'Economy' GROUP BY aircraft_code;").show()
    spark.sql("SELECT aircraft_code, COUNT(fare_conditions) FROM seatsAirplane WHERE aircraft_code = '773' and fare_conditions = 'Comfort' GROUP BY aircraft_code;").show()
    spark.sql("SELECT aircraft_code, COUNT(fare_conditions) FROM seatsAirplane WHERE aircraft_code = '773' and fare_conditions = 'Business' GROUP BY aircraft_code;").show()


    spark.sql("SELECT aircraft_code, COUNT(fare_conditions) FROM seatsAirplane WHERE aircraft_code = 'CN1' and fare_conditions = 'Economy' GROUP BY aircraft_code;").show()
    

def showAll(dfs: Dict):
    table_df: DataFrame
    dfs["boarding_passes"].show()

    for key, table_df in dfs.items():
        print(f"Table name: {key}")
        table_df.show()
    
    
def main():
    spark = initSession()
    dfs = loadTables(spark)
    
    # printSchemas(dfs)
    seatsInfo(dfs["seats"], spark)
    
    
    
    # showAll(dfs)
    # makeFullRelations(dfs)
    


main()