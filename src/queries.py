######################################################################
# Methods for executing neo4j queries
######################################################################

from constants import DRIVER

def executeQuery(query):
    with DRIVER.session() as session:
        try:
            session.run(query)
        except Exception as e:
            print("Error executing query:\n")
            print(query)
