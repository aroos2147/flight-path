import util.queries as q;
from util.constants import SPARK;

def main():
    SPARK.sparkContext.setLogLevel("ERROR")
    result = q.getTopKCities(10)
    result.show()

if __name__ == "__main__":
    main()