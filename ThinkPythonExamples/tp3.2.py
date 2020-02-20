#write a function that takes a function object and a value as arguments

def print_twice(arg): # print an argument twice
    print(arg)
    print(arg)

def do_twice(func, arg): # does a function twice
    func(arg)
    func(arg)

def do_four(func, arg): # does a function four times using a twice function
    do_twice(func, arg)
    do_twice(func, arg)

word = "salem"
do_twice(print_twice, word)

print(' ')

do_four(print_twice, "salem")
