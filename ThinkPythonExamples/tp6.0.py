import math

def hypotenuse(a,b):
    c = math.sqrt(a**2 + b**2)
    return c

def area(radius):
    return math.pi * radius**2

def distance(x1,y1,x2,y2):
    return math.sqrt((x2-x1)**2+(y2-y1)**2)

def circle_area(sx, yc, xp, yp):
    return area(distance(xc, yc, xp, yp))

def is_divisible(x,y):
    return x % y == 0

def is_between(x, y, z):
    return x <= y <= z

def factorial(n):
    if n==0:
        return 1
    else:
        recurse = factorial(n-1)
        result = n * recurse
        return result

def fibonacci(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fibonacci(n-1) + fibonacci(n-2)

def factorial(n):
    if not isinstance(n,int):
        print('Factorial is only defined for integers.')
        return None
    elif n < 0:
        print('Factorial is not defined for negative integers.')
        return None
    else:
        return n * factorial(n-1)
