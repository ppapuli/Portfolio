# Function that checks whether three side lengths are capable of forming a triangle
def is_triangle(s1,s2,s3):
    if s1 > s2+s3 or s2 > s1+s3 or s3 > s1+s2:
        print("No")
    else: print("Yes")

# Prompt user for the lengths of the triangle
sideA = int(input("What is your first length? \n"))
sideB = int(input("And your second? \n"))
sideC = int(input("What about your third? \n"))

#call the function
is_triangle(sideA,sideB,sideC)
