import math
import turtle

bob = turtle.Turtle()


def iso_triangle(t, length, angle1, angle2, angle3):
    """Draws a triangle with given length and angle.

    t: Turtle
    length: length of edges
    angle: angle between edges
    """
    y = length * math.sin(angle1*math.pi/180)/math.sin(angle2*math.pi/180) # break iso triangle down the middle into two 45-45-90 triangles and solve using sin(theta)=o/h
    t.lt(angle1)
    t.fd(length)
    t.lt(angle2)
    t.fd(y)
    t.lt(angle3)
    t.fd(length)

def turtle_pie(t, length, c_angle, o_angle, num_triangle):
    """ t: Turtle
        length: length of the equal sides
        c_angle: central angle (angle opposite non-equal leg)
        o_angle: outside angle (angle opposite equal leg)
        num_triangle: number of triangles in turtle pie
        """
    c_angle = 360/num_triangle
    o_angle = 90 + (c_angle/2)
    for i in range (num_triangle):
        iso_triangle(t, length, c_angle, o_angle, o_angle)
        bob.lt(180-c_angle)
    bob.pu()
    bob.bk(130) # moves the turtle object backwards to draw another tpie
    bob.pd()

c_angle = 1 # declare vars
o_angle = 1
turtle_pie(bob, 50, c_angle, o_angle, 5)
turtle_pie(bob, 50, c_angle, o_angle, 6)
turtle_pie(bob, 50, c_angle, o_angle, 7)
turtle_pie(bob, 50, c_angle, o_angle, 8)

turtle.mainloop()
