import turtle
import math

bob = turtle.Turtle()
alice = turtle.Turtle()

def square(t, l):
    for i in range(4):
        t.fd(l)
        t.lt(90)

def polyline(t, n, length, angle):
    for i in range(n):
        t.fd(length)
        t.lt(angle)

def polygon(t, n, length, angle):
     angle = 360.0 / n
     polyline(t, n, length, angle)

def circle(t, r):
    arc(t, r, 360)

def arc(t, r, arc_angle): # creates an arc based on a ratio of circumference to a full circle by creating arc segmentations based on the arc length and resolution
    circumference = 2 * math.pi * r
    arc_length = circumference * (arc_angle / 360)
    num_side = int(arc_length / 3) + 1
    edge_angle = arc_angle / num_side
    arc_segment = arc_length / num_side
    polyline(t, n, arc_segment, edge_angle)

#print("Enter your desired number of sides: ")
#num_side = int(input())
print("Enter your desired radius: ")
radius = int(input())
print("Enter your desired arc angle: ")
arc_angle = int(input())
#edge_angle = 360 / num_side
#length = 2
#polygon(bob, length, angle, num_side)
#circle(bob, radius)
arc(bob, radius, arc_angle)
turtle.mainloop()
