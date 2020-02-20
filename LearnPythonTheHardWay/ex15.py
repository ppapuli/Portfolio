from sys import argv #imports the module argv from sys

script, filename = argv #set the arguments for argv module

txt = open(filename) #set variable that opens the filename argument from argv

print(f"Here's your file {filename}:") #formatted string text showing you what you will see
print(txt.read()) #prints the text from the filename opened

txt.close() #close the filename text. Very important to close files when done

print("Type the filename again:") #Prompts user with an action
file_again = input("> ") #set new variable defined by user input

txt_again = open(file_again) #same as txt (Line 5)

print(txt_again.read()) #prints the text from a user inputted filename

txt_again.close() #same as Line 10
