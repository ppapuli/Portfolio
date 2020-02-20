# Experimenting with the string methods at the following:
#http: // docs. python. org/ 3/library/ stdtypes. html# string-methods
# .strip and .replace are particularly useful

myString = "   My Name is Peter Name   "

'''print(myString.capitalize()) # capitalize the first character, the rest are lowercase
print(myString.casefold()) # removes all case distinctions
print(myString.count("e", 0, 15)) # counts the number of substring from [start] to [end]
print(myString.encode(encoding="utf-8", errors="strict")) # encode using UTF-8
print(myString.endswith(("My", "Name", "Peter"))) # returns true if the string ends with a substring, can be a tuple
print(myString.expandtabs(tabsize=8)) # replaces tab characters with spaces
print(myString.strip(" ")) # strip any extra whitespace at the beginning and end of a string
print(myString.replace("Name", "Cake"))
print(myString.swapcase())'''

# tp8.2.py
fruit = "banana"
print(fruit.count("a"))
print(myString.islower())
