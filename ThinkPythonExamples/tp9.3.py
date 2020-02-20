fin = open('words.txt')

# prints true if word U frbdn_lttrs = 0
def avoids(word, frbdn_lttrs):
    for letter in word:
        if letter in frbdn_lttrs:
            return False
    return True


frbdn_lttrs = ['a', 'b', 'c', 'd', 'e']
print(frbdn_lttrs[-1])

def iterate(case):
    for index in range(5):
        while ord((frbdn_lttrs[-1*index])) <= 123:
            case5 = ord((frbdn_lttrs[-1*index]))
            print(case5)
            iterate(case5+1)







'''frbdn_lttrs = input("What is your string of forbidden characters? \n >")
count = 0
for line in fin:
    if avoids(line, frbdn_lttrs):
        print(line)
        count += 1'''
