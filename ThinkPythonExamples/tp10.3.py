# deletes the first and last element in a list and returns the new list
def middle(lst):
    del lst[0]
    lngth = len(lst)
    del lst[lngth-1]
    return lst

t = [1,2,3,4]
print(middle(t))
