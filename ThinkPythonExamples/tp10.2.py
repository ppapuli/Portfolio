def cumsum(t):
    """Computes the cumulative sum of the numbers in t.

    t: list of numbers

    returns: list of numbers
    """
    total = 0
    res = []
    for x in t:
        total += x
        res.append(total)
    return res


t = [1, 2, 3]
print(cumsum(t))
