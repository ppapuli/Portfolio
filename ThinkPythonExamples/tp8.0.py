'''index = 0
while index < len(fruit):
    letter = fruit[index]
    print(letter)
    index += 1
'''

'''# iterate through a string to print from the last letter to the first.
newIndex = 0
fruit = input('> ')
while abs(newIndex) < len(fruit):
    letter = fruit[-newIndex-1]
    print(letter)
    newIndex += 1
'''

'''# concatenation (string addition) and a for loop to generate an alphabetical series.
prefixes = ('J', 'K', 'L', 'M', 'N', 'Ou', 'P', 'Qu')
suffix = 'ack'

for letter in prefixes:
    print(letter + suffix)
'''

'''# String Slices... the operator [n:m] returns the string from n(inclusive) to m(exclusive)
fruit = 'banana'
print(fruit[:3]) # prints 'ban'
print(fruit[3:]) # prints 'ana'
print(fruit[3:3]) # prints '', [n,m] has a difference of zero, so the cursor doesn't move anywhere, thus printing an empty string
print(fruit[:]) # prints 'banana'

# Strings are immutable...
greeting = 'Hello, World!'
greeting[0] = 'J'  #Give a TypeError, because you can't change values in a string Object
new_greeting = 'J' + greeting[1:] # To change the H to a J you add J to the front and the rest of the greeting starting from the second character
print(new_greeting)   # Prints Jello, World
'''

'''# Search for a letter in a word and prints the index where it first occurs
def find(word, letter, index): # word = word to search, letter = letter to search for in 'word', index = where to start looking
    for i in range(len(word)): # create a loop that iterates within the number of letters in the word
        if word[index] == letter: # if the indexed letter in 'word' matches the letter, return the index where that word occurs
            return index
        index += 1 # increment to the next iteration of the loop
    return -1 # if nothing is found, return -1

print(find("banana", "n", 0))'''

'''# Counts the numebr of times a letter appears in a string
word = 'banana'
count = 0 # initilize our counter variable
letter_input = input() # asks user for input on which letter to check
for letter in word: # iterates through each letter in the word
    if letter == letter_input: # checks if the letter in the index matches the inputted letter
        count += 1 # increment the counter if it does match, then iterate onto the next index
print(count)
'''
'''# A generalized function of the previous code
def count(word, letter):
    count = 0
    for index in word:
        if index == letter:
            count += 1
    print(count)

count("onomatopoeia", "o")
'''

'''# 8.8 String Methods
word = 'banana'
new_word = word.upper() #takes the string and returns a new string with all uppercase letters
print(new_word)
'''

'''word = 'banana'
index = word.find('na', 3)
print(index)'''

'''# 8.9 'in' operator takes two strings and returns true if the first appears as a substring in the second
fruit = ('banana', 'apple', 'orange')
print('banana' in fruit[0:1])

# prints all letters from word1 that appear in word2
def in_both(word1, word2):
    for letter in word1:
        if letter in word2:
            print(letter)

in_both('some', 'awesome')'''

'''# 8.10 String Comparison
if word == 'banana':
    print('All right, bananas.')

if word < 'banana':
    print('Your word, ' + word + ', comes before banana.')
elif word > 'banana':
    print('Your word, ' + word + ', comes after banana.')
else:
    print('All right, bananas.')'''

'''# 8.11 debugging... the following function has two errors
def is_reverse(word1, word2):
        #takes one string argument and returns true
        #if the second string argument is the reverse
        #of the first
    if len(word1) != len(word2):
        return False

    i = 0
    j = len(word2)

    while j > 0:    # the first error is here: the condition of the while statement should read '>=' instead of '>' since j[0] is the first letter of the second word
        if word1[i] != word2[j]:    # the second error is here: running with this will give an IndexError because the value of 'j' is 4, the second word's index ends at 3
            return False
        i += 1
        j -= 1
    return True

is_reverse('pots', 'stop')'''
