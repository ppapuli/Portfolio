def recurse(n, s):
    ''' recursively adds a positive integer with all integers before it up to zero
        n: number to be recursively added
        s: sum of all integers from the current recursion until n = 0
    '''
    if n == 0:
        print(s)
    else:
        recurse(n-1, n+s)

recurse(10, 0)
