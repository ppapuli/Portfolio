fin = open('words.txt')

# prints true if all letters in the string are used at least once in the word argument
def uses_all(word, string):
    for letter in string:
        if letter not in word:
            return False
    return True


count = 0

for line in fin:
    if uses_all(line, "aeiou"):
        print(line)
        count += 1

print(count)   
