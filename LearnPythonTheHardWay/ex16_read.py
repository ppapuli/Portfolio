from sys import argv

script, filename = argv

txt = open(filename)

print(f"Let me show you the contents of {filename}:")
print(txt.read())

txt.close()

print("I forget, what was that filename again?")
file = input("It was ")

print("Oh yeah!")
newtxt = open(file)
print(newtxt.read())

newtxt.close()
