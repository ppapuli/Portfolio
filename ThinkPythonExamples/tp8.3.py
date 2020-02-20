def isPalindrome(word):
    '''returns true if the word is a palindrome'''
    return word == word[::-1] # returns true if the word read forwards is the same as the word read backwards

print(isPalindrome('racecars'))
