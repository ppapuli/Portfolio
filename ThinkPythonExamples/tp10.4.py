def chop(t):
    del t[0]
    del t[-1]

def is_sorted(t):
    temp = t[:]
    temp.sort()
    if temp == t:
        return True
    return False
