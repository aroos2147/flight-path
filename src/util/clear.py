######################################################################
# Script for clearing database contents
######################################################################

import util.queries as queries

if __name__ == "__main__":
    queries.executeQuery("MATCH (n) DETACH DELETE n")