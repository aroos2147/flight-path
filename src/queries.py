######################################################################
# Methods for executing neo4j queries
######################################################################

from constants import DRIVER, SPARK, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

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
                .format("ne04j")
                .option("url", NEO4J_URI)
                .option("authentication.type", "basic")
                .option("authentication.basic.username", NEO4J_USER)
                .option("authentication.basic.password", NEO4J_PASSWORD)
                .option("labels", ":" + name)
                .load())

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