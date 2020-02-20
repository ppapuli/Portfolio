# non memoized ackerman function
def ack(m,n):
    #Guardian Program
    if not isinstance(n,int):
        print('Factorial is only defined for integers.')
        return None
    elif n < 0:
        print('Factorial is not defined for negative integers.')
        return None


    elif m == 0:
        return n+1
    elif  m > 0 and n == 0:
        return ack(m-1, 1)
    elif m > 0 and n > 0:
            return ack(m-1, ack(m,n-1))



# memoized ackerman function
cache = {}

def ackermann(m, n):
    """Computes the Ackermann function A(m, n)

    See http://en.wikipedia.org/wiki/Ackermann_function

    n, m: non-negative integers
    """
    if m == 0:
        return n+1
    if n == 0:
        return ackermann(m-1, 1)

    if (m, n) in cache:
        return cache[m, n]
    else:
        cache[m, n] = ackermann(m-1, ackermann(m, n-1))
        return cache[m, n]


print(ackermann(3, 4))
print(ackermann(3, 6))
