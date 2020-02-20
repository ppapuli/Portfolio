# prints true if the word has three consecutive double letter pairings
def car_puzzle(word):
    i = 0
    count = 0
    while i < len(word) - 1:
        if word[i] == word[i+1]:
            count += 1
            if count == 3:
                return True
            i += 2
        else:
            count = 0
            i += 1
    return False

fin = open('words.txt')
counter = 0

for line in fin:
    if car_puzzle(line):
        print(line)
        counter += 1

print(counter)
