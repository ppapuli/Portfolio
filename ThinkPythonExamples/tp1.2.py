''' How many seconds are there in 42 minutes and 42 seconds? '''

#function to convert any hours and minutes to seconds
def time_in_seconds(hours = 0, minutes = 0, seconds = 0):
    if hours + minutes + seconds == 0:
        hours = float(input("How many hours? "))
        minutes = float(input("How many minutes? "))
        seconds = float(input("How many seconds? "))
    converted_time = (hours * 3600) + (minutes * 60) + (seconds * 1)
    print(f"There are {converted_time} seconds in {hours} hours, {minutes} minutes, and {seconds} seconds. ")

hours = 0 # if you know the amount of hours, insert here
minutes = 0 # if you know the amount of minutes, insert here
seconds = 0 # if you know the amount of seconds, insert here

time_in_seconds(hours,minutes,seconds)
