#this one is like your scripts with argv
def print_two(*args): #define the function that prints two arguments
    arg1, arg2 = args #everything after the indent is part of the function
    print(f"arg1: {arg1}, arg2: {arg2}") #print it in a formatted string

#ok, that *args is actually pointless, we can just do this
def print_two_again(arg1, arg2): #same thing as previous function, but defines both variables in the definition of the function
    print(f"arg1: {arg1}, arg2: {arg2}")

#this just takes one argument
def print_one(arg1): #defines a function with one var
    print(f"arg1: {arg1}")

#this one takes no arguments
def print_none(): #defines a function with no var
    print("I got nothin'.")

#print the functions with defined variables
print_two("Zed","Shaw")
print_two_again("Zed","Shaw")
print_one("First!")
print_none()
