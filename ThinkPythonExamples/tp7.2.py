import math

def eval_loop():
    while True:
        result = input('> ')
        if result == 'done':
            break
        else:
            print(eval(result))
    return print(result)

eval_loop()
