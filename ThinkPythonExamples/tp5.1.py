import time
epoch = time.time() # number of seconds since January 1, 1970

print(epoch)

# convert seconds since epoch into minutes, hours and days
minutes = epoch / 60
hours = epoch / (60*60)
days = epoch / (60*60*24)
#print (minutes, hours, days)

# convert time since epoch into time since today started
today_days = days-(epoch//(60*60*24)) # percentage of todays completion in decimal
today_hours = today_days*24
today_minutes = today_hours*60
today_seconds = today_minutes*60
#print(today_days, today_hours, today_minutes, today_seconds)

today_time = print(int(today_hours//1), "\b:", int(today_minutes%60), "\b:", "\b", int(today_seconds%60))

print(today_time)
