def isPower(a, b):
    "Returns true if 'a' is a power of 'b'"
    if a % b == 0 and (a / b) % b == 0:
        return print(True)
    else:
        return print(False)

isPower(int(input()),int(input()))
