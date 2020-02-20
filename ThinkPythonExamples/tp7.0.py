# prints something three times using recursion
def print_n(s, n):
    if n <= 0:
        return
    print(s)
    print_n(s, n-1)

# recreation of print_n but through iteration instead of recursion
def iterationPrint_n(s, n):
    while n > 0:
        print(s)
        n -= 1

# takes a number "n" and counts down to 0
def countdown(n):
    while n > 0:
        print(n)
        n -= 1
    print('Blastoff!')

# Collatz Conjecture showing that terminating a while statement isn't always clear
def sequence(n):
    while n != 1:
        print(n)
        if n % 2 == 0:
            n = n/2
        else:
            n = n*3 + 1

# using the break statement
while True:
    line = input('> ')
    if line == 'done':
        break
    print(line)
print('Done!')


while True:
    print(x)
    y = (x + a/x) / 2
    if abs(y-x) < epsilon:
        break
    x = y
