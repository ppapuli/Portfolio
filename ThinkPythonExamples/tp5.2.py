def check_fermat(a,b,c,n):
    if a**n + b**n == c**n:
        if n > 2:
            print("Holy smokes, Fermat was wrong!")
        else:
            print("No, that doesn't work.")
    else:
        print(f"{a**n} + {b**n} does not equal {c**n}")

print("Let's see if Fermat's last theorem holds!\n")
a = int(input("What's your value of a? \n"))
b = int(input("What's your value of b? \n"))
c = int(input("What's your value of c? \n"))
n = int(input("What's your value of n? \n"))
check_fermat(a,b,c,n)
