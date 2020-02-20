from sys import argv #import argv from sys
from os.path import exists #returns True if file exists, false if it does not

script, from_file, to_file = argv #set arguments for argv

print(f"Copying from {from_file} to {to_file}")

#we could do these two on one line, how?
in_file = open(from_file).read() #open the file we will take the text from and create variable with file object
'''indata = in_file.read()''' #set variable to the data inside the file object

'''print(f"The input file is {len(indata)} bytes long") #print the length of the file in bytes

print(f"Does the output file exist? {exists(to_file)}") #use exists module to see if the file we will write to exists, will print true if it does
print("Ready, hit Return to continue, CTRL-C to abort.") #tell user how to abort
input() #user input, return continues'''

out_file = open(to_file, 'a') #open the file you will write to, 'a' will add to the end, 'w' will truncate
out_file.write(in_file) #write the text in 'indata' into 'outfile'

'''print("Alright, all done.")'''

#close the files we no longer use
out_file.close()
#in_file.close()
