def has_duplicates(lst):    # returns true if there are two items in a list that are equal
    newlist = []
    for char in lst:
        if char in newlist:
            return True
        newlist.append(char)
    return False
