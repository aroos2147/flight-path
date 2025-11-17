######################################################################
# Script for transforming and loading the data into neo4j
######################################################################

import queries
import csv
import time
from constants import COUNTRIES_PATH, AIRLINES_PATH, AIRPORTS_PATH, ROUTES_PATH
from fancyPrints import printGreen

countryCount = 0
airlineCount = 0
airportCount = 0
routesCount = 0

def main():
    global countryCount
    global airlineCount
    global airportCount

    start = time.perf_counter()
    print("Loading countries...")
    loadFromFile(COUNTRIES_PATH, loadCountry)
    printGreen(f"Loaded {countryCount} countries successfully in {(time.perf_counter() - start):.3f}s")

    start = time.perf_counter()
    print("Loading airlines...")
    loadFromFile(AIRLINES_PATH, loadAirline)
    printGreen(f"Loaded {airlineCount} airlines successfully in {(time.perf_counter() - start):.3f}s")

    start = time.perf_counter()
    print("Loading airports...")
    loadFromFile(AIRPORTS_PATH, loadAirport)
    printGreen(f"Loaded {airportCount} airports successfully in {(time.perf_counter() - start):.3f}s")
    
    start = time.perf_counter()
    print("Loading routes...")
    loadFromFile(ROUTES_PATH, loadRoute)
    printGreen(f"Loaded {routesCount} routes successfully in {(time.perf_counter() - start):.3f}s")

# Loads a line from "countries.dat" into the "Country" node type
def loadCountry(line):
    global countryCount
    countryCount += 1

    name = line[0]
    isoCode = line[1]
    dafifCode = line[2]
    query = f"CREATE (c:Country {{name: '{name}', isoCode: '{isoCode}', dafifCode: '{dafifCode}'}})"

    queries.executeQuery(query)

# Loads a line from "airlines.dat" into the "Airline" node type,
# and creates a "LOCATED_IN" relationship to the associated country
def loadAirline(line):
    # The first line is junk, and the second "Private flight" airline is never used.
    if (line[1] == "Unknown" or line[1] == "Private flight"):
        return

    global airlineCount
    airlineCount += 1

    id = line[0]
    name = line[1]
    alias = line[2]
    iata = line[3]
    icao = line[4]
    callsign = line[5]
    country = line[6]
    active = "true" if line[7] == "Y" else "false" 
    query = f"""
        MATCH (c:Country {{ name: '{country}'}})
        CREATE (a:Airline {{
            id: {id},
            name: '{name}',
            alias: '{alias}',
            iata: '{iata}',
            icao: '{icao}',
            callsign: '{callsign}',
            active: {active}
        }})
        CREATE (a)-[:LOCATED_IN]->(c)
        """ if country != "" else f"""
        CREATE (a:Airline {{
            id: {id},
            name: '{name}',
            alias: '{alias}',
            iata: '{iata}',
            icao: '{icao}',
            callsign: '{callsign}',
            active: {active}
        }})
        """
    
    queries.executeQuery(query)

# Loads a line from "airlines.dat" into the "Airline" node type,
# and creates a "LOCATED_IN" relationship to the associated country
def loadAirport(line):
    global airportCount
    airportCount += 1

    id = line[0]
    name = line[1]
    city = line[2]
    country = line[3]
    iata = line[4]
    icao = line[5]
    latitude = line[6]
    longitude = line[7]
    altitude = line[8]
    timeZone = line[9]
    dst = line[10]
    tzTimeZone = line[11]

    query = f"""
        MATCH (c:Country {{ name: '{country}'}})
        CREATE (a:Airport {{
            id: {id},
            name: '{name}',
            city: '{city}',
            iata: '{iata}',
            icao: '{icao}',
            latitude: {latitude},
            longitude: {longitude},
            altitude: {altitude},
            timeZone: '{timeZone}',
            dst: '{dst}',
            tzTimeZone: '{tzTimeZone}'
        }})
        CREATE (a)-[:LOCATED_IN]->(c)
        """
    
    queries.executeQuery(query)

def loadRoute(line):
    global routesCount
    routesCount += 1

    airlineId = line[1]
    sourceId = line[3]
    destId = line[5]
    codeshare = line[6] 
    stops = line[7]
    equipment = line[8]
    queryParts = []

    # All three of these fields can be blank (represented with a \N), so
    # the query has to be constructed accordingly
    if (sourceId != "\\N"):
        queryParts.append(f"MATCH (src:Airport {{ id: {sourceId}}})")
    if (destId != "\\N"):
        queryParts.append(f"MATCH (dest:Airport {{ id: {destId}}})")
    if (airlineId != "\\N"):
        queryParts.append(f"MATCH (airline:Airline {{ id: {airlineId}}})")

    queryParts.append(f"CREATE (r:Route {{codeshare: '{codeshare}', stops: {stops}, equipment: '{equipment}'}})")

    if (sourceId != "\\N"):
        queryParts.append(f"CREATE (src)-[:OUTBOUND]->(r)")
    if (destId != "\\N"):
        queryParts.append(f"CREATE (r)-[:INBOUND]->(dest)")
    if (airlineId != "\\N"):
        queryParts.append(f"CREATE (r)-[:USES]->(airline)")

    query = '\n'.join(queryParts)

    queries.executeQuery(query)

# Executes a processing function on each line of the given csv file
def loadFromFile(path, processFunc):
    with open(path, 'r', encoding = 'utf-8') as file:
        reader = csv.reader(file)

        for line in reader:
            processFunc(line)

if __name__ == "__main__":
    main()