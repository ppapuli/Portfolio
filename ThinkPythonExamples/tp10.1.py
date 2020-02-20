'''def nested_sum(intList):
    nSum = 0
    index = 0
    for char in intList:
        if type(intList[index]) is list:
            n = sum(intList[index])
            nSum += n
        else:
            nSum += intList[index]
        index += 1
    return nSum

print(nested_sum([1,[2,1,1],[1,2,3]]))
'''

'''
lst = [1,2,[1,2,3]]
n = lst[2]
nlst = sum(n)
print(nlst)
olst = lst[0] + lst[1]
print(nlst + olst)
'''

def nested_sum(t):
    """Computes the total of all numbers in a list of lists.
    t: list of list of numbers
    returns: number
    """
    total = 0
    for nested in t:
        total += sum(nested)
    return total


t = [[1, 2], [3], [4, 5, 6]]
print(nested_sum(t))
