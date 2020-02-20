def rotate_letter(letter, n):
    """Rotates a letter by n places.  Does not change other chars.

    letter: single-letter string
    n: int

    Returns: single-letter string
    """
    if letter.isupper():
        start = ord('A')
    elif letter.islower():
        start = ord('a')
    else:
        return letter

    c = ord(letter) - start
    i = (c + n) % 26 + start
    return chr(i)


def rotate_word(word, n):
    """Rotates a word by n places.

    word: string
    n: integer

    Returns: string
    """
    res = ''
    for letter in word:
        res += rotate_letter(letter, n)
    return res


fin = open('words.txt')

def dictionary(f):
    words_dict = {}

    for line in fin:
        word = line.strip()
        words_dict[word] = None
    return words_dict


library = dictionary(fin)


def has_rotation(d):
    rotations = {}
    for key in d:
        for n in range(1,25):
            rotated = rotate_word(key, n)
            if rotated in d:
                #print('key', key, '\trotated', rotated, '\tn', n)
                rotations.setdefault(key, [rotated, n])
    return rotations


print(has_rotation(library))
