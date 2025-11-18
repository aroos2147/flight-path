import util.queries as q;
from util.constants import SPARK;

def main():
    airports = q.getActiveAirlinesInCountry("United States")
    airports.show()

if __name__ == "__main__":
    main()