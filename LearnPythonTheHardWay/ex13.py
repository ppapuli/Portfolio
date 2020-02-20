from sys import argv # import module "argument variable" from the system
# read the WYSS section for how to run this
script, first, second, third = argv # defines the arguments unpacked from argv

first = input("What is your age? ") # ask user for input on what first is
print("The script is called:", script)
print("Your first variable is:", first)
print("Your second variable is:", second)
print("Your third variable is:", third)
