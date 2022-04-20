import turtle
import math
bob = turtle.Turtle()

def draw_polyLine(turt,n,d,ang):
    for i in range(n):
      turt.fd(d)
      turt.lt(ang)

def draw_poly(turt,d,n):
    deg = 360/n
    draw_polyLine(turt,n,d,deg)

def draw_cicrle(turt,r):
    circum = math.pi * 2 * r
    sides = int((circum/6)+3)
    stepL = int(circum/sides)
    draw_poly(turt,stepL,sides)

#draw_cicrle(bob,50)

def draw_arc(turt,r,ang):
    circum = math.pi *2 * r * ang / 360
    sides = int((circum/6)+3)
    stepL = circum/sides
    stepA = ang/sides

    draw_polyLine(turt,sides,stepL,stepA)

draw_arc(bob,40,180)



turtle.mainloop()