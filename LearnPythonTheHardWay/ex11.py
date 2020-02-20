print("How old are you?", end=' ')
age = input() #prompts the user for an input
print("How tall are you?", end=' ')
height = input()
print("How much do you weigh?", end=' ')
weight = input()

print(f"So, you're {age} old, {height} tall and {weight} heavy.") #take the input from the user and print it back very cleverly

name = input("What's your name? ") #ask for input which is inserted into the variable 'name'
print(f"Nice to meet you {name}!") #formatted string with the variable name inside the string

colors = input("What are your favorite colors? ")
print(f"Wow {name}, so your favorite colors are {colors}!\nI know so much about you now")


x = int(input("What's your age again? ")) #asks the user for their age in a string, which is then converted into an integer number
birthyear = 2018 - x #calculate their birthyear by subtracting their age from current year
print(f"So you were born in {birthyear}?")
