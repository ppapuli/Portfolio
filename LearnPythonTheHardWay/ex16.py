from sys import argv #import module argv from sys

script, filename = argv #set the two arguments in argv

print(f"We're going to erase {filename}.") #formatted string showing which file user will erase
print("If you don't want that, hit CTRL-C (^C).") #action will end program
print("If you do want that, hit RETURN.") #return will continue program

input("?") #ask for user input on if we should truncate the file

print("Opening the file...") #string text
target = open(filename, 'w') #set variable to file object in 'write' mode, while also truncating the file

print("Truncating the file. Goodbye!") #string text
target.truncate() #erase the contents of the file // 'w' already does this automattically

print("Now I'm going to ask you for three lines.") #string text

#user input that will be written to the file
line1 = input("line 1: ")
line2 = input("line 2: ")
line3 = input("line 3: ")

print("I'm going to write these to the file.") #string text

#write variable 'line x' to file object set in variable 'target'
target.write(line1 + "\n" + line2 + "\n" + line3 + "\n") #one command instead of six
'''target.write("\n")
target.write(line2)
target.write("\n")
target.write(line3)
target.write("\n")'''

print("And finally, we close it.") #string text
target.close() #close text file since we no longer need it
