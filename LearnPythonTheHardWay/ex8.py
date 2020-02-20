#Set up an array with 4 columns and 1 row
formatter = "{} {} {} {}"

print(formatter.format(1, 2, 3, 4)) #Put 1-4 into the formatter variable
print(formatter.format("one", "two", "three", "four")) # Put the strings 1-4 into the formatter variable
print(formatter.format(True, False, False, True)) #same with boolean logic
print(formatter.format(formatter, formatter, formatter, formatter)) #same with a variable
print(formatter.format(
    "Try your",
    "Own text here",
    "Maybe a poem",
    "Or a song about fear" #again with strings
))
