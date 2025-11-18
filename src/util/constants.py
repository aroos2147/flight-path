######################################################################
# Collection of constant values for easy reference
######################################################################

from neo4j import GraphDatabase
from pyspark.sql import SparkSession

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password_1234"

AIRLINES_PATH = "data/airlines.dat"
AIRPORTS_PATH = "data/airports.dat"
COUNTRIES_PATH = "data/countries.dat"
ROUTES_PATH = "data/routes.dat"

DRIVER = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
SPARK = (SparkSession.builder
            .appName("FlightPath")
            .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3")
            .getOrCreate())
