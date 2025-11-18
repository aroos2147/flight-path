######################################################################
# Methods for printing colored text to the terminal
######################################################################

RED_CODE = "\033[31m"
GREEN_CODE = "\033[32m"
DEFAULT = "\033[0m"

COLORS = [
    "black",
    "red",
    "green",
    "yellow",
    "blue",
    "magenta",
    "cyan",
    "white"
]

def printGreen(message):
    printColored(message, "green")

def printRed(message):
    printColored(message, "red")

def printColored(message, text = "white", background = "black"):
    print(constructCode(background, 40) + constructCode(text, 30) + message + DEFAULT)

def constructCode(color, offset):
    try:
        index = COLORS.index(color)
    except ValueError:
        index = -1

    code = offset + index if index >= 0 else 0

    return "\033[" + str(code) + "m"