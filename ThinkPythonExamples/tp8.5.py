'''def rotateWord(s, num):
    for letter in s:    # for every letter in the string
        letterCode = ord(letter)    # assigns the numeric code for each letter
        newLetter = chr(letterCode + num) # adds the rotation to the numeric code and converts it back into a character
        print(newLetter) # prints the converted character


rotateWord('cheer', 7)
'''

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


print(rotate_word('cheer', 7))
#print(rotate_word('melon', -10))
#print(rotate_word('sleep', 9))
