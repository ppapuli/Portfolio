from random import randint
import time
from bisect import bisect_left

# Creates a list of 'n' people and assigns a random birthday symbolized by a number from 1 to 365
def birthday_assignment(n):
    birthday_room = []
    for i in range(n):
        birthday_room.append(randint(1,365))
    return (birthday_room)

#Sorts the birthdays and adds a counter if there are any duplicates, returns the probability of a birthday pair within a sample size
def bday_duplicates(n, sample):
    duplicate_counter = 0
    for i in range(sample):   # iterate throughout the sample size to see how many times a sample produces a duplicate based on the 'n' sized room
        room = sorted(birthday_assignment(n))   # sorts the room from lowest number to highest
        if has_duplicates(room):   # increment the duplicate counter if there is a duplicate
            duplicate_counter += 1
    return (duplicate_counter/sample)

# populates a list with words.txt by using the append command
def word_append():
    word_list = []
    fin = open('words.txt')
    for line in fin:
        word = line.strip()
        word_list.append(word)
    fin.close()
    return word_list

# populates a list by adding each element in words.txt
def word_add():
    word_list = []
    fin = open('words.txt')
    for line in fin:
        word = line.strip()
        word_list += [word]
    fin.close()
    return word_list

 # takes a sorted list and returns the index that a target word exists, uses the bisect module
def bisect_cheat(word_list, word):
    i = bisect_left(word_list, word)
    if i == len(word_list):
        return False
    return word_list[i] == word

# takes a sorted list and returns True if the target word exists in the bisect
def in_bisect(word_list, word):
    if len(word_list) == 0:
        return False
    bisector = len(word_list) // 2

    if word_list[bisector] == word:
        return True

    if word_list[bisector] > word:
        return in_bisect(word_list[:bisector], word)

    if word_list[bisector] < word:
        return in_bisect(word_list[bisector+1:], word)

# returns true if a word in 'words.txt' has a reverse pair
def reverse_pair(word_list, word):
    rev_word = word[::-1]
    return in_bisect(word_list, rev_word)

# returns the interlocked version of two stringed words
def interlock(lock_one, lock_two):
    locked_word = []
    combined_length = len(lock_one + lock_two) // 2
    for index in range(combined_length):
        #print('index', index)
        locked_word.append(lock_one[index])
        #print('locked word', locked_word)
        locked_word.append(lock_two[index])
        #print('locked word', locked_word)
    return lock_one, lock_two

# returns true if a word in the words.txt is an interlocked word
def interlocked(t, word):
    lock1 = []
    lock2 = []

    # populate the empty lock lists with the locked elements
    for index in range(len(word)):
        if index % 2 == 0:
            lock2.append(word[index])
            #print('lock2', lock2)
        else:
            lock1.append(word[index])
            #print('lock1', lock1)

    lock1 = ''.join(lock1)  # convert the lists back to string using .join
    lock2 = ''.join(lock2)

    #if both of the locking words are in words.txt, we return True
    if in_bisect(t, str(lock1)) and in_bisect(t, str(lock2)):
        print(word, 'is an interlocked word')
        print(lock1, 'is an interlockable word. Just like: ', lock2, 'is an interlockeable word\n')
        return interlock(lock1, lock2)

'''
t = word_add()
counter = 0
for line in t:
    if interlocked(t, line):
        counter += 1
print('the number of interlocked words is: ', counter)

'''
'''start_time = time.time()
print(bisect('anaconda'))
elapsed_time = time.time() - start_time
print(elapsed_time)
'''


'''
start_time = time.time()
t = word_append()
elapsed_time = time.time() - start_time

print(len(t))
print(t[:10])
print(elapsed_time, 'seconds')

start_time = time.time()
t = word_add()
elapsed_time = time.time() - start_time

print(len(t))
print(t[:10])
print(elapsed_time, 'seconds')
'''
#birthday_assignment(4)
#print(bday_duplicates(23, 1000))


#lst = [2,1,2,3,4,5]
#bday_paradox(23, 1000)


#print(has_duplicates(lst))
#print(is_anagram("Tom Marvolo Riddle",  "I am Lord Voldemort"))


def interlock(word_list, word):
    """Checks whether a word contains two interleaved words.

    word_list: list of strings
    word: string
    """
    evens = word[::2]
    odds = word[1::2]
    return in_bisect(word_list, evens) and in_bisect(word_list, odds)


def interlock_general(word_list, word, n=3):
    """Checks whether a word contains n interleaved words.

    word_list: list of strings
    word: string
    n: number of interleaved words
    """
    for i in range(n):
        inter = word[i::n]
        if not in_bisect(word_list, inter):
            return False
    return True


if __name__ == '__main__':
    word_list = word_add()

    for word in word_list:
        if interlock(word_list, word):
            print(word, word[::2], word[1::2])

    for word in word_list:
        if interlock_general(word_list, word, 3):
            print(word, word[0::3], word[1::3], word[2::3])
