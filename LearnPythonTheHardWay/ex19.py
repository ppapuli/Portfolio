def cheese_and_crackers(cheese_count, boxes_of_crackers): #define a function
    print(f"You have {cheese_count} cheeses!") #formatted string
    print(f"You have {boxes_of_crackers} boxes of crackers!") #formatted string
    print("Man that's enough for a party!")
    print("Get a blanket.\n")


print("We can just give the function numbers directly:")
cheese_and_crackers(20,30) #numbers can be inputted directly into the function


print("OR, we can use variables from our script:") #create new variables to use in the function
amount_of_cheese = input("How much cheese do you want: ")
amount_of_crackers = input("How many crackers: ")

cheese_and_crackers(amount_of_cheese, amount_of_crackers) #variables go into the fucntion


print("We can even do math inside too:")
cheese_and_crackers(10 + 20, 5 + 6) #or you can do math


print("And we can combine the two, variables and math:") #or combine math and new variables
cheese_and_crackers(int(amount_of_cheese) + 100, int(amount_of_crackers) + 1000)
