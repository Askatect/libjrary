from datetime import datetime
import math
import numpy as np
import random
import pyautogui as pag
from pynput import keyboard

def circle(c, scrwidth, scrheight):
    t = 2*np.pi*(c % 100)/100
    r = scrheight/3
    return scrwidth/2 + r*math.cos(t), scrheight/2 + r*math.sin(t), 0

def rect(c, scrwidth, scrheight):
    c = c % 100
    if c < 12.5:
        return 5*scrwidth/6, scrheight*(0.5 + c/37.5), 0
    elif c < 37.5:
        return scrwidth*(5/6 - 2*(c - 12.5)/75.0), 5*scrheight/6, 0
    elif c < 62.5:
        return scrwidth/6, scrheight*(5/6 - 2*(c - 37.5)/75.0), 0
    elif c < 87.5:
        return scrwidth*(1/6 + 2*(c - 62.5)/75.0), scrheight/6, 0
    else:
        return 5*scrwidth/6, scrheight*(1/6 + (c - 87.5)/37.5), 0

def rndm(c, scrwidth, scrheight):
    return random.randint(0, scrwidth), random.randint(0, scrheight), random.triangular(1, 5, 2)

def on_press(key):
    print(f'{key} press detected at {datetime.now().time()}.')
    return

def on_release(key):
    print(f'{key} release detected at {datetime.now().time()}.')
    if key == keyboard.Key.esc:
        global jiggle
        jiggle = False
        print('Listener stopping - jiggling will cease.')
        return False

scrwidth, scrheight = pag.size()

listener = keyboard.Listener(on_press = on_press, on_release = on_release)
listener.start()

jiggle = True
c = 0

while jiggle:
    x, y, d = circle(c, scrwidth, scrheight)
    y = scrheight - y 
    pag.moveTo(x, y, duration = d)
    c += 1