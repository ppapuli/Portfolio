fin = open('words.txt')

# prints true if all the letters in the word only contains letters used in the string
def uses_only(word, string):
    for letter in word:
        if letter not in string:
            return False
    return True

print(uses_only("hello face", " acefhlo "))
