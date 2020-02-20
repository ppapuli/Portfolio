fin = open('words.txt')

# returns true if the word does not have the letter 'e'
def has_no_e(s):
    for letter in s:
        if letter == 'e':
            return False
    return True

# counts the number of words without an 'e' in words.txt
count = 0
for line in fin:
    word = line.strip()
    if has_no_e(line):
        print(line)
        count += 1

# counts the number of words in words.txt
fin.close() # closes the file object
fin = open('words.txt') # reopens to start from the beginning
linecounter = 0
for line in fin: # iterates through the entire text object
    word = line.strip()
    fin.readline() # reads each line
    linecounter += 1 # increment the counter

print(count/linecounter * 100) # prints the percentage of words in words.txt that don't have the letter 'e'
