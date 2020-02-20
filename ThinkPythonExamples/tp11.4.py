import time

def has_duplicates(lst):    # returns true if there are two items in a list that are equal
    newlist = []
    for char in lst:
        if char in newlist:
            return True
        newlist.append(char)
    return False

# same as has_duplicates except uses a dictionary
def has_dupeDic(lst):
    book = {}
    index = 0
    for index in lst:
        val = lst[index-1]
        if val in book:
            return True
        book.setdefault(val, 1)
    return False

this_list = [1,2,3,4,5,1]

# calculates the elapsed time for the dictionary method of duplicates in a list
start_time = time.time()

print(has_dupeDic(this_list))

elapsed_time = time.time() - start_time
print('total elapsed time for dictionary', elapsed_time)

start_time = time.time()

print(has_duplicates(this_list))

elapsed_time = time.time() - start_time
print('total elapsed time for list', elapsed_time)
