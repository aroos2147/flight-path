import util.queries as q;
from util.constants import SPARK;

def main():
    airports = q.getAirports()
    airports.show()

if __name__ == "__main__":
    main()