import math

signal_power = 100
noise_power = 0.1
ratio = signal_power / noise_power
decibels = 10 * math.log10(ratio)
print (decibels)

radians = 1/2 * math.pi
height = math.sin(radians)
print (height)

def print_twice(line1, line2):
    print(line1+line2)
    print(line1+line2)
    return int(math.sin(math.pi/2))

result = print_twice('Bing ', 'Bang ')
print(result)
