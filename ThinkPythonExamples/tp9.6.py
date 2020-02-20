fin = open('words.txt')

# compare adjacent letters to see if the subsequent is higher in the alphabet
def is_abecedarian(word):
    previous = word[0]
    for c in word:
        if c < previous:
            return False
    return True

# does the same thing but using recursion
def is_abecedarian_recursive(word):
    if len(word) <= 1:
        return True
    if word[0] > word[1]:
        return False
    return is_abecedarian(word[1:])

# does the same thing using a while loop
def is_abecedarian_while(word):
    i = 0
    while i < len(word) - 1:
        if word[i+1] < word[i]:
            return False
        i += 1
    return True


print(is_abecedarian('avb'))
print(is_abecedarian_recursive('bbc'))
print(is_abecedarian_while('avb'))
print(bool('A' < 'a')) # you can do a bool check for letters
