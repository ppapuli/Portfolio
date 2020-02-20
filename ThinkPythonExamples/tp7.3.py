import math

def factorial(n):
    """Computes factorial of n recursively."""
    if n == 0:
        return 1
    else:
        recurse = factorial(n-1)
        result = n * recurse
        return result

def estimate_pi():
    summation = 0
    k = 0
    prefix = 2*math.sqrt(2)/9801
    while True:
        num = factorial(4*k) * (1103 + 26390*k)
        den = factorial(k)**4 * 396**(4*k)
        iteration = num / den
        summation += iteration
        k += 1
        if abs(iteration) < 1e-15:
            break
    print(1/(prefix*summation))

estimate_pi()
