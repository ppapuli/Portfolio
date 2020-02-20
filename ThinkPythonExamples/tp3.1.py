# Write a function named right_justify that takes a string named s as a parameter
# and prints the string with enough leading spaces so that the last letter of the string is in column 70
# of the display.

def right_justify(word):
    wordLength = len(word)
    num_space = 70 - wordLength
    print (' '* num_space, word)

print ("Enter a value you would like to right justify! ")
value = input()
right_justify(value)
