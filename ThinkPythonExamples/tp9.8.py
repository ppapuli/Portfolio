# takes a number, converts it into a string and tells you if that string number is a palindrome
def is_palindrome(num):
    j = str(num)                        # set variable for the string of the number, so that we can iterate through its digits, one by one
    index = -1                              # set the index that will iterate through the string number from the last digit towards the middle
    for digit in j:                             # digit iterates through the numstring, acting as a comparitive index
        if digit != j[index]:                 # checks if the two pointers are equal to each other
            return False                          # if they are not, the number can't be a palindrome, otherwise iterate to the next numbers
        elif len(j) / 2 < abs(index):
            break
        index -= 1
    return True

# converts the number to a string, and returns true if the last four digits satisfy the function
def last_four(num):
    numstring = str(num)                #print('numstring is \t', numstring)
    index = len(numstring) - 4              #print('index is \t', index)
    lastfour = numstring[index:]                #print('lastfour is\t', lastfour)
    if is_palindrome(lastfour):                     #print(is_palindrome(lastfour))
        return True

def last_five(num):
    numstring = str(num)
    index = len(numstring) - 5
    lastfive = numstring[index:]
    if is_palindrome(lastfive):                # print(is_palindrome(lastfive))
        return True

def middle_four(num):
    numstring = str(num)
    index = len(numstring) - 5
    index2 = len(numstring) - 1
    middlefour = numstring[index:index2]       # print('the middle four digits are\t', middlefour)
    if is_palindrome(middlefour):               #    print(is_palindrome(middlefour))
        return True


def carPuzzle():
    i = 100000
    for i in range(999999):
        if last_four(i):
            i += 1
            if last_five(i):
                i += 1
                if middle_four(i):
                    i += 1
                    if is_palindrome(i):
                        print(i-3)
                        print(i-2)
                        print(i-1)
                        print(i)

print(carPuzzle())

#print(is_palindrome('1111'))
#print(middle_four(112216))



#VARIABLE TESTS    (copy and paste)
'''
# is_palindrome var tests:
     print('digit\t', digit)
      print('index\t', index)
     print('i\t', j[index - 1])
'''
