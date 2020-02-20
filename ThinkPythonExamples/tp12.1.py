# Write a function called most_frequent that takes a string and prints the letters in decreasing
# order of frequency. Find text samples from several different langauges and see how letter frequency
# varies between languages.

def histogram(s):
    hist = {}
    for c in s:
        hist[c] = hist.get(c, 0) + 1
    return hist

def most_frequent(s):
    hist = histogram(s)

    t = []
    for c, freq in hist.items():
        t.append((freq, c))

    t.sort(reverse=True)

    res = []
    for freq, c in t:
        res.append(c)

    return res

def read_file(filename):
    return open(filename).read()


string = read_file('words.txt')
letter_seq = most_frequent(string)
for x in letter_seq:
    print(x)
