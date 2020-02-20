fin = open('words.txt')

def dictionary(f):
    words_dict = dict()

    for line in fin:
        word = line.strip()
        words_dict[word] = 0
    print('aardvark' in words_dict)




print(dictionary(fin))
