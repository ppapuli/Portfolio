# This script will read a text file to clean up a decklist to import into Cockatrice

# Type the name of your text file that will be manipulated
#----------------------------------------------------------#

textfile = 'Marwyn.txt'

#----------------------------------------------------------#



# open the file so we can write and alter
file = open(textfile, 'r+')

# define a function that will truncate the line argument past the first open parenthesis
def TextSplit(line):
    sep = '('
    txt = line
    return txt.split(sep, 1)[0]

# create a dummy array that will hold our data
cleanText = []
# import all of our cleaned up line data into an array
for line in file:
    cleanText.append(TextSplit(line) + '\n')

# first erase the previous data, then rewrite using the data saved in the array
file.truncate(0)
for line in cleanText:
    file.write(line)


# Close the file once we are done so it doesn't use memory
file.close()
