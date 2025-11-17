######################################################################
# Script for clearing database contents
######################################################################

import queries

if __name__ == "__main__":
    queries.executeQuery("MATCH (n) DETACH DELETE n")