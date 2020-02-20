#the total amount of cars
cars = 100
#how many people can fit in a single car
space_in_a_car = 4.0
#the drivers that are able to drive a car
drivers = 30
#the passengers that will need a driver
passengers = 90
#cars that will be idle
cars_not_driven = cars - drivers
#cars that will not be idle
cars_driven = drivers
#amount of passengers that can be transported today
carpool_capacity = cars_driven * space_in_a_car
#the average passengers in each car
average_passengers_per_car = passengers / cars_driven


print("There are", cars, "cars available.")
print("There are only", drivers, "drivers available.")
print("There will be", cars_not_driven, "empty cars today.")
print("We can transport", carpool_capacity, "people today.")
print("We have", passengers, "to carpool today.")
print("We need to put about", average_passengers_per_car, "in each car.")
