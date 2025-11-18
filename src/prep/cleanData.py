######################################################################
# Script for clearing database contents
######################################################################

import re
from util.constants import COUNTRIES_PATH, AIRLINES_PATH, AIRPORTS_PATH

def main():
    formatQuoteMarks(COUNTRIES_PATH)
    formatQuoteMarks(AIRLINES_PATH)
    formatQuoteMarks(AIRPORTS_PATH)

def formatQuoteMarks(path):
    with open(path, 'r', encoding = 'cp850') as file:
        content = file.read()
        pattern = re.compile("[\\\\]*'")
        replaced = re.sub(pattern, "\\'", content)
    
    with open(path, 'w', encoding = 'cp850') as file:
        file.write(replaced)

if __name__ == "__main__":
    main()