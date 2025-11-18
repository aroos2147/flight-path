import util.queries as q;
from util.constants import SPARK;

def main():
    SPARK.sparkContext.setLogLevel("ERROR")
    q.getShortestTrip("1", "2")

if __name__ == "__main__":
    main()