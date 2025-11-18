######################################################################
# Methods for executing neo4j and spark operations
######################################################################

from util.constants import DRIVER, SPARK, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from graphframes import GraphFrame
from pyspark.sql.functions import col, asc, desc, coalesce

#######################################################################
# BASIC
#######################################################################

# Executes a generic query on the neo4j database
def executeQuery(query):
    with DRIVER.session() as session:
        try:
            session.run(query)
        except Exception as e:
            print("Error executing query:\n")
            print(query)

# Loads all nodes of the given name into a Spark dataframe
def getNodes(name):
    return (SPARK.read
                .format("neo4j")
                .option("url", NEO4J_URI)
                .option("authentication.type", "basic")
                .option("authentication.basic.username", NEO4J_USER)
                .option("authentication.basic.password", NEO4J_PASSWORD)
                .option("labels", ":" + name)
                .load())

# Loads all edges from the source type to the target type with the 
# given relationship into a Spark dataframe
def getEdges(source, target, relationship):
    return (SPARK.read
                .format("neo4j")
                .option("url", NEO4J_URI)
                .option("authentication.type", "basic")
                .option("authentication.basic.username", NEO4J_USER)
                .option("authentication.basic.password", NEO4J_PASSWORD)
                .option("relationship", relationship)
                .option("relationship.source.labels", ":" + source)
                .option("relationship.target.labels", ":" + target)
                .load())

def getByQuery(query):
    return (SPARK.read
                .format("neo4j")
                .option("url", NEO4J_URI)
                .option("authentication.type", "basic")
                .option("authentication.basic.username", NEO4J_USER)
                .option("authentication.basic.password", NEO4J_PASSWORD)
                .option("query", query)
                .load())

#######################################################################
# CORE DATAFRAMES
#######################################################################

# Loads the neo4j Airport nodes into a Spark dataframe
def getAirports():
    return getNodes("Airport")

# Loads the neo4j Airline nodes into a Spark dataframe
def getAirlines():
    return getNodes("Airline")

# Loads the neo4j Country nodes into a Spark dataframe
def getCountries():
    return getNodes("Country")

# Loads the neo4j Route nodes into a Spark dataframe
def getRoutes():
    return getNodes("Route")

# Loads the neo4j OUTBOUND edges into a Spark dataframe
def getOutboundRoutes():
    return getEdges("Route", "Airport", "OUTBOUND")

# Loads the neo4j OUTBOUND edges into a Spark dataframe
def getInboundRoutes():
    return getEdges("Airport", "Route", "INBOUND")

# Loads the neo4j airport LOCATED_IN edges into a Spark dataframe
def getAirportsLocatedIn():
    return getEdges("Airport", "Country", "LOCATED_IN")

# Loads the neo4j airline LOCATED_IN edges into a Spark dataframe
def getAirlinesLocatedIn():
    return getEdges("Airline", "Country", "LOCATED_IN")

# Loads the neo4j USES edges into a Spark dataframe
def getAirlineUses():
    return getEdges("Route", "Airline", "USES")

#######################################################################
# ALGORITHMS
#######################################################################

# Gets all airports in the given country
def getAirportsInCountry(country):
    return (getByQuery(f"MATCH (a:Airport)-[:LOCATED_IN]->(:Country{{name:\"{country}\"}}) RETURN a")
            .select("a.*")
            .select("name", "altitude", "latitude", "longitude", "city", "iata", "icao", "timeZone"))

# Gets all routes with X amount of stops
def getRoutesWithStops(stops):
    return (getByQuery(f"MATCH (s:Airport)-[:OUTBOUND]->(r:Route{{stops:{stops}}})-[:INBOUND]->(d:Airport) RETURN s, r, d")
            .select(col("r.<id>").alias("id"),
                    col("s.name").alias("srcName"),
                    col("s.icao").alias("srcCode"),
                    col("d.name").alias("destName"),
                    col("d.icao").alias("destCode"),
                    col("r.stops").alias("stops")))

# Gets all routes that use codeshare
def getRoutesWithCodeShare():
    return (getByQuery("MATCH (s:Airport)-[:OUTBOUND]->(r:Route{codeshare:\"Y\"})-[:INBOUND]->(d:Airport) RETURN s, r, d")
            .select(col("r.<id>").alias("id"),
                    col("s.name").alias("srcName"),
                    col("s.icao").alias("srcCode"),
                    col("d.name").alias("destName"),
                    col("d.icao").alias("destCode"),
                    col("r.codeshare").alias("codeshare")))

# Gets all currently active airlines in a given country
def getActiveAirlinesInCountry(country):
    return (getByQuery(f"MATCH (a:Airline{{active:true}})-[:LOCATED_IN]->(c:Country{{name:\"{country}\"}}) RETURN a")
            .select("a.*")
            .select("name", "iata", "icao", "callsign", "alias"))

# Gets the country with the most airports
def getCountryWithMostAirports():
    counts = getByQuery("MATCH (:Airport)-[:LOCATED_IN]->(c:Country) RETURN c.name as country, count(*) AS airportCount")
    top = counts.orderBy(desc("airportCount")).collect()[0]

    return (top['country'], top['airportCount'])

# Gets the top K cities with the most incoming/outgoing routes
def getTopKCities(k):
    cityToCity = getByQuery("MATCH (s:Airport)-[:OUTBOUND]->(r:Route)-[:INBOUND]->(d:Airport) RETURN s.city AS srcCity, d.city AS destCity")
    outCounts = cityToCity.groupBy("srcCity").count().withColumnRenamed("count", "outCount")
    inCounts = cityToCity.groupBy("destCity").count().withColumnRenamed("count", "inCount")
    combined = (outCounts.join(inCounts, outCounts.srcCity == inCounts.destCity, how="outer")
        .withColumn("city", col("srcCity")) # Could also be destCity or a coalesce, but there are no null or empty city names so this works
        .withColumn("total", col("outCount") + col("inCount"))
        .select("city", "outCount", "inCount", "total"))
    
    return combined.orderBy(desc("total")).limit(k)
