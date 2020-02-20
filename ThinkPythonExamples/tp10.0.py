# Lists are mutable, can be changed

numbers = [43, 123]

empty = [0]

cheeses = ['Cheddar', 'Edam', 'Gouda']
print('Edam' in cheeses)

# 10.3 Traversing a List

for cheese in cheeses:  # good method to read the elements of a List
    print(cheese)

for i in range(len(numbers)):   # len returns the number of elements in the list
    numbers[i] = numbers[i] * 2  # traverses the list and updates each element of numbers
print(numbers[:])

print(cheeses)
cheeses[1:] = ['Brie', 'Mozzarella']

cheeses.sort()
print(cheeses)


# Map, Filter, and Reduce
def add_all(t):
    total = 0
    for x in t:
        total += x
    return total

#the above function is built into the function sum, which is a reduction
t = [1,2,3]
sum(t)

#maps "map" a function onto each of the elements on a sequence
def capitalize_all(t):
    res = []
    for s in t:
        res.append(s.capitalize())
    return res

# filters iterate through a list and return a sublist
def only_upper(t):
    res = []
    for s in t:
        if s.isupper():
            res.append(s)
    return res

# to delete elements from a list, you can use 'pop' if you know the index
t = ['a', 'b', 'c']
x = t.pop(1)
print(t)
# if you know the element but not the index, you can use remove
t.remove('c')
print(t)
# to remove more than one element, you can use del with a slice index
t = ['a', 'b', 'c', 'd', 'e', 'f']
del t[1:5]
print(t)

# if you want to convert a string into a list of characters
s = 'spam'
t = list(s)
print(t)
# if you want to break a string into words, you can use the split method
y = 'pining for the fjords'
t = y.split()
print(t)

# using a 'delimiter' allows you to split a string using a boundary for a word
s = 'spam-spam-spam'
delimiter = '-'
t = s.split(delimiter)
print(t)

# join is the inverse of split, it concatenates the elements.
t = ['pining', 'for', 'the', 'fjords']
delimiter = ' '
s = delimiter.join(t)
print(s)
