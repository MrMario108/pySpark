from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
print(sys.version_info)
def initSession() -> SparkSession:
    spark: SparkSession
    spark = SparkSession.builder.appName("SQLite_Spark").config("spark.jars", "src/jars/sqlite-jdbc-3.49.1.0.jar").getOrCreate()
    spark.version
    return spark

def loadTables(spark: SparkSession) -> Dict:
    # Ścieżka do bazy SQLite
    database_path = "src/airlines/newTravels.sqlite"

    # Definiujemy parametry JDBC
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
    
def amountFlights(df: DataFrame, spark: SparkSession):
    df.createOrReplaceTempView("amountFlights")

    # pokaż wszystko
    spark.sql("SELECT * from amountFlights").show()

    # pokaż ilość lotów w BD
    spark.sql("SELECT COUNT(DISTINCT flight_id) from amountFlights").show()

    # pokaż ilość połączeń
    spark.sql("SELECT COUNT(DISTINCT flight_no) from amountFlights").show()

    # pokaż sumę za bilety dla lotów
    spark.sql("SELECT flight_id, SUM(amount) AS suma from amountFlights GROUP BY flight_id ORDER BY suma desc").show()

    # sprawdzenie poprawności poprzedniego zapytania
        #ilość biletów
    spark.sql("SELECT COUNT(*) AS tickets_number FROM amountFlights WHERE flight_id = '2354'").show()
    
    spark.sql("SELECT flight_id, amount FROM amountFlights WHERE flight_id = '2354'").show()
    
    spark.sql("SELECT flight_id, SUM(amount) AS suma FROM amountFlights WHERE flight_id = '2354' GROUP BY flight_id").show()

    # pokaż sumę za bilety dla lotów mniejszą niż 50.000
    spark.sql("SELECT flight_id, SUM(amount) AS suma from amountFlights GROUP BY flight_id HAVING suma < 50000 ORDER BY suma desc").show()

    # ile jest takich wyników
    spark.sql(
            """WITH tickets_value 
                (SELECT flight_id, SUM(amount) AS suma from amountFlights GROUP BY flight_id HAVING suma < 50000)
                SELECT count(*) FROM tickets_value"""
            ).show()



    

def getAmountFlightsDF(dfs: Dict) -> DataFrame:

    flights: DataFrame
    tickets: DataFrame
    bookings: DataFrame
    tickets_flight: DataFrame

    flights = dfs["flights"]
    tickets = dfs["tickets"]
    bookings = dfs["bookings"]
    tickets_flight = dfs["ticket_flights"]


    df = (
        flights
        .join(tickets_flight, flights.flight_id == tickets_flight.flight_id, "left")
        .drop(tickets_flight.flight_id)
        .join(tickets, tickets_flight.ticket_no == tickets.ticket_no, "left")
        .drop(tickets.ticket_no)
        .join(bookings, tickets.book_ref == bookings.book_ref, "left")
    )
    return df

def showAll(dfs: Dict):
    table_df: DataFrame
    dfs["boarding_passes"].show()

    for key, table_df in dfs.items():
        print(f"Table name: {key}")
        table_df.show()
    
    
def main():
    spark = initSession()
    dfs = loadTables(spark)
    
    amountFlights(getAmountFlightsDF(dfs), spark)

    # printSchemas(dfs)
    # seatsInfo(dfs["seats"], spark)
    # showAll(dfs)
    # makeFullRelations(dfs)
    


main()