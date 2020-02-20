import xlrd

# feed the file path and open the workbook
loc = ("C:/Users/Peter.AWL/Desktop/AWL Contacts 2019.xlsx")
data = xlrd.open_workbook(loc)
sheet = data.sheet_by_index(0)

num_employees = 47

# iterate through an excel contact list and create a list with all employees first and last names, stored as String Tuples: ('Peter', 'Papuli')
def employeeList():
    employees = list()
    for i in range(num_employees):
        fname, lname = sheet.cell_value(i+4, 1), sheet.cell_value(i+4, 2)
        employees.append((fname, lname))
    return employees

# returns the number associated with a letter of the alphabet
def letterCode(lr):
    letter_to_num = {}  # create an open dictionary
    for code in range(1, 26):
        character = chr(code + 64)  # chr is used to convert any number to its ASCII character. 65 is the code to offset to "A".
        if code < 10:
            code = str(code).zfill(2)
        letter_to_num[character] = code  # create entries with the character as a key and the corresponding number as the value (letter is an index)
    return letter_to_num[lr]

# create a dictionary with every numerical value based on keyboard position
keyboardColumn = {'A':1, 'B':5, 'C':3, 'D':3, 'E':3, 'F':4, 'G':5, 'H':6, 'I':8, 'J':7, 'K':8, 'L':9, 'M':7, 'N':6, 'O':9, 'P':0, 'Q':1, 'R':4, 'S':2, 'T':5, 'U':7, 'V':4, 'W':2, 'Z':2, 'Y':6, 'Z':1}
# Create a dictionary with special character values based on the number on the keyboard
shiftNumber = {'1':'!', '2':'@', '3':'#', '4':'$', '5':'%', '6':'^', '7':'&', '8':'*', '9':'(', '0':')'}

def passwordEncode(fname, lname):
    # Create the attributes required for the password's conditions
    empFirstInit = employee[0][0][0]
    firstInitNum = str(keyboardColumn[employee[0][0][0]])
    empLastInit = employee[1][0][0]
    lastInitNum = str(keyboardColumn[employee[1][0][0]])
    firstInitCode = str(letterCode(empFirstInit))
    lastInitCode= str(letterCode(empLastInit))
    empShiftNum = str((int(firstInitCode) + int(lastInitCode))).zfill(2)
    tensShiftNum = empShiftNum[0]
    tensShiftCode = shiftNumber[tensShiftNum]
    onesShiftNum = empShiftNum[1]
    onesShiftCode = shiftNumber[onesShiftNum]

    # Add the employee passed as an argument to a dictionary with all passwords stored
    if empFirstInit < 'N':
        passwords[employee] = 'awl' + empFirstInit + firstInitNum + empLastInit + lastInitNum + firstInitCode + lastInitCode + tensShiftCode + onesShiftCode
    else:
        passwords[employee] = empFirstInit + firstInitNum + empLastInit + lastInitNum + firstInitCode + lastInitCode + tensShiftCode + onesShiftCode + 'awl'
    return passwords


employees = employeeList()
passwords = {}
for employee in employees:
    passwordEncode(employee, employee)

fname, lname = input(), input()
print(fname), print(lname)
# we want to print the password for anybody whose name we enter
print(passwords['Peter', 'Papuli'])



#print(LtoN())

