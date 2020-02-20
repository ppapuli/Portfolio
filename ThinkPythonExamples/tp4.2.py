import math
import turtle

maru = turtle.Turtle()

def polyline(t, n, length, angle):
    for i in range(n):
        t.fd(length)
        t.lt(angle)

def arc(t, r, angle): # uses polyline to draw an arc of a circle
    circumference = 2 * math.pi * r # distance around the circle edge
    arc_length = circumference * angle / 360 # how far around the circle the arc "travels"
    segment_num = int(arc_length / 3 + 1) # how many segments we should break the arc into
    arc_segment = arc_length / segment_num # the length of one arc segment
    segment_angle = angle / segment_num # what angle to turn the turtle after every arc segment
    polyline(maru, segment_num, arc_segment, segment_angle)


def petal(t, r, angle): # draws a petal using the arc function
    for i in range(2):
        arc(t, r, angle)
        t.lt(180-angle)

def flower(t, r, angle, num_petal): # draws a flower using the petal function for a specified number of petals
    for i in range(num_petal):
        petal(t, r, angle)
        maru.lt(360/num_petal) # turns the turtle an amount relative to the number of petals

flower(maru, r=50, angle=60, num_petal=10)

maru.hideturtle()
turtle.mainloop()
