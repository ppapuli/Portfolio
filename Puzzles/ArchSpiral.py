import math
import turtle

def polyline(t, n, length, angle):
    """Draws n line segments.

    t: Turtle object
    n: number of line segments
    length: length of each segment
    angle: degrees between segments
    """
    for i in range(n):
        t.fd(length)
        t.lt(angle)

def arc(t, r, angle):
    """Draws an arc with the given radius and angle.

    t: Turtle
    r: radius
    angle: angle subtended by the arc, in degrees
    """
    arc_length = 2 * math.pi * r * abs(angle) / 360 # ratio of arc in relation to circumference of a circle
    n = int(arc_length / 4) + 3 # number of drawable pieces to break the arc into
    step_length = arc_length / n # breaks the arc length into a number of small forward lengths
    step_angle = float(angle) / n # the angle needed to turn after each small forward step

    # making a slight left turn before starting reduces
    # the error caused by the linear approximation of the arc
    t.lt(step_angle/2)
    polyline(t, n, step_length, step_angle)
    t.rt(step_angle/2)

def spiral(t, r, angle):
    """ Draws an archimedes Spiral rotation using increasing radius of an arc.

    t: Turtle
    r: radius
    angle: angle that spiral subtends in degrees
    """
    for i in range(20):
        arc(bob, 5*i, 90) # draws small arcs with increasing radius to make a spiral


#if __name__ == '__main__':
bob = turtle.Turtle()


#arc(bob, 50, 180)
#arc(bob, 100, 180)S
spiral(bob, 105, 360)



    # wait for the user to close the window
turtle.mainloop()
