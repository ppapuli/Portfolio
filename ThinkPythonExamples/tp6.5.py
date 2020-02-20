def gcd(a, b):
    r = a % b
    if a % b == 0:
        return print(b)
    else:
        gcd(b, r)

def decor(func):
    try:
        func(a,b)
    except ZeroDivisionError:
        print("Can't mod by zero")


decor(gcd(int(input()), int(input())))



'''
print(120%45) # 120 = a , 45 = b , 30 = r
print(45%30) # 45 = a , 30 = b , 15 = r
print(30%15) # 30 = a , 15 = b , 0 = r
'''
