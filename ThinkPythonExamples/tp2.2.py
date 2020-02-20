# volume of a sphere with radius 5
pi = 3.141592
r = 5
vol_sphere = 4/3 * pi * r**3
print (int(vol_sphere))

# wholesale cost for 60 copies of a book
book_price = 24.95 # in dollars
bookstore_discount = 0.6 # 40 percent off
first_shipping_cost = 3 # dollars
additional_shipping_cost = 0.75 # dollars
num_copies = 60

wholesale_cost = num_copies * (book_price * bookstore_discount) + ((num_copies - 1) * additional_shipping_cost) + first_shipping_cost
print (int(wholesale_cost))

# time spent running
easy_pace_min = 8
easy_pace_sec = 15

tempo_pace_min = 7
tempo_pace_sec = 12

start_time_hr = 6
start_time_min = 52
start_time_sec = 0

end_time_hr = start_time_hr
end_time_min = start_time_min + easy_pace_min*2 + tempo_pace_min*3
end_time_sec = easy_pace_sec*2 + tempo_pace_sec*3

if end_time_sec >= 60:
    end_time_sec = end_time_sec % 60
    end_time_min += 1

if end_time_min >= 60:
    end_time_min = end_time_min % 60
    end_time_hr += 1

if end_time_hr >= 24:
    end_time_hr = end_time_hr % 24

# end_time = str(end_time_hr) + str(end_time_min) + str(end_time_sec)
print(f"After leaving the house at 6:52am, you will get home at {end_time_hr}:{end_time_min}:{end_time_sec}")
