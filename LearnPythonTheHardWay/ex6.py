#set variable
types_of_people = 10
#set a variable with a formatted string
x = f"There are {types_of_people} types of people."

#set more variables
binary = "binary"
do_not = "don't"
#formatted string using the two previous string variables
y = f"Those who know {binary} and those who {do_not}."

#print for everyone to see
print(x)
print(y)

#print a formatted string with variable x
print(f"I said: {x}")
#print another formatted string with variable y
print(f"I also said: '{y}'")

#set hilarious = to a logic False
hilarious = False
joke_evaluation = "Isn't that joke so funny?! {}"
#print previous variable with the logic format of False
print(joke_evaluation.format(hilarious))

#define left and right side of string
w = "This is the left side of..."
e = "a string with a right side."

#print said string
print(w + e)
