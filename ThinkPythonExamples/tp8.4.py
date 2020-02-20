string = "my "

def any_lowercase(s): # prints true if the first string is lowercase
    for c in s:
        if c.islower():
            return True
        else:
            return False

def any_lowercase2(s): # prints true if the character 'c' is lowercase
    for c in s:
        if 'c'.islower():
            return 'True'
        else:
            return 'False'

def any_lowercase3(s): # prints true if the last character is lowercase
    for c in s:
        flag = c.islower()
    return flag

def any_lowercase4(s): # prints true if any character is lowercase
    flag = False
    for c in s:
        flag = flag or c.islower()
    return flag

def any_lowercase5(s): # prints true if all characters are lowercase without a space
    for c in s:
        if not c.islower():
            return False
    return True

print("The base case is:", string.islower())
print(any_lowercase(string))
print(any_lowercase2(string))
print(any_lowercase3(string))
print(any_lowercase4(string))
print(any_lowercase5(string))
