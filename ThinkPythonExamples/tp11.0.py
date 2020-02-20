# traverses a string and populates a dictionary with the frequency of each letter
def histogram(s):
    d = dict()
    for c in s:
        if c not in d:
            d[c] = 1
        else:
            d[c] += 1
    return d

# same as histogram, but uses the get method
def histogram_get(s):
    d = dict()
    for c in s:
        if c in d:
            d[c] += 1
        else:
            d[c] = d.get(c, 1)
    return d

# searches a dictionary 'd' for a value 'v' (bad for performance)
def reverse_lookup(d, v):
    for k in d:
        if d[k] == v:
            return k
        raise LookupError('value does not appear in the dictionary')

# inverts a dictionary. if there are multiple values per key, we create a list of letters
def invert_dict(d):
    inverse = dict()
    for key in d:
        val = d[key]
        if val not in inverse:
            inverse[val] = [key]
        else:
            inverse[val].append(key)
    return inverse

# a fibonacci function using a dictionary to store known values
known = {0:0, 1:1}
def fibonacci(n):
    if n in known:
        return known[n]

    res = fibonacci(n-1) + fibonacci(n-2)
    known[n] = res
    return res



#print(fibonacci(12))
h = histogram('parrot')
#print(invert_dict(h))
