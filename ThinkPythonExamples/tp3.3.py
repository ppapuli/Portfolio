# write a function that produces a grid, outlined on page 49 of think python 2

'''def plus():
    print('+', end = ' ')

def minus():
    print('- ' * 4, end = ' ')

def bar():
    print('|', end = ' '*10)

def rowPlus():
    plus(), minus(), plus(), minus(), plus()

def rowBars():
    bar(), bar(), bar()
    print('\n')
    bar(), bar(), bar()
    print('\n')
    bar(), bar(), bar()
    print('\n')

rowPlus(), print("\n"), rowBars(), rowPlus(), print("\n"), rowBars(), rowPlus()
'''
# write a similar function that instead has 4 rows and 4 columns
def do_four(func):
    do_twice(func)
    do_twice(func)

def do_twice(f):
    f()
    f()

def plus():
    print('+', end = ' ')

def minus():
    print('- ' * 4, end = ' ')

def bar():
    print('|', end = ' '*10)

def rowPlus():
    plus(), minus(), plus(), minus(), plus(), minus(), plus(), print('\n')

def rowBars():
    do_four(bar)
    print('\n')
    do_four(bar)
    print('\n')
    do_four(bar)
    print('\n')


rowPlus(), rowBars(), rowPlus(), rowBars(), rowPlus(), rowBars(), rowPlus()
