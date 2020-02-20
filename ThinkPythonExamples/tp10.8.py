from random import randint

def has_duplicates(s):
    """Returns True if any element appears more than once in a sequence.
    s: string or list
    returns: bool
    """
    # make a copy of t to avoid modifying the parameter
    t = list(s)
    t.sort()

    # check for adjacent elements that are equal
    for i in range(len(t)-1):
        if t[i] == t[i+1]:
            return True
    return False


'''create a list of 'n' participants with a randomly generated bday'''
def bdays_in_room(n):
    birthday_list = []  # create an empty list that will become populated
    for i in range(n):
        birthday_list.append(randint(1,365))    # assign a random integer from 1 to 365, signifying each day of the year
        #print(birthday_list) '''test'''
    return sorted(birthday_list)

'''sort the birthday list and determine if there are any duplicates'''
def bday_paradox(n, sample):
    count = 0
    for i in range(sample):
        t = bdays_in_room(n)
        if has_duplicates(t):
            count += 1
        print('count', count)



print(bday_paradox(4,10))
