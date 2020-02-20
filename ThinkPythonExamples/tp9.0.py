# Case Study: Word Play

# 9.1 Reading Word Lists
fin = open('words.txt')
for line in fin:
    word = line.strip()
    print(word)
