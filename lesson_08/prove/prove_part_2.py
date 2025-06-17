"""
Course: CSE 351 
Assignment: 08 Prove Part 2
File:   prove_part_2.py
Author: <Add name here>

Purpose: Part 2 of assignment 8, finding the path to the end of a maze using recursion.

Instructions:
- Do not create classes for this assignment, just functions.
- Do not use any other Python modules other than the ones included.
- You MUST use recursive threading to find the end of the maze.
- Each thread MUST have a different color than the previous thread:
    - Use get_color() to get the color for each thread; you will eventually have duplicated colors.
    - Keep using the same color for each branch that a thread is exploring.
    - When you hit an intersection spin off new threads for each option and give them their own colors.

This code is not interested in tracking the path to the end position. Once you have completed this
program however, describe how you could alter the program to display the found path to the exit
position:

What would be your strategy?
I would keep track of the steps each thread is taking as it goes through the maze. I could call a list within
explore to add each square into. Whenever a thread finds the exit, you can take that list and change it into a
global variable like winning path, which I can then draw at the end in its own special color.

Why would it work?

Because every thread is keeping track of its trail, when the thread finds the exit, we have the route that it took.
Then we just copy that route into a new variable and color the route differently. I believe that
it would need to be copied over before all the threads stop to avoid overwrites. 

"""

import math
import threading 
from screen import Screen
from maze import Maze
import sys
import cv2

# Include cse 351 files
from cse351 import *

SCREEN_SIZE = 700
COLOR = (0, 0, 255)
COLORS = (
    (0,0,255),
    (0,255,0),
    (255,0,0),
    (255,255,0),
    (0,255,255),
    (255,0,255),
    (128,0,0),
    (128,128,0),
    (0,128,0),
    (128,0,128),
    (0,128,128),
    (0,0,128),
    (72,61,139),
    (143,143,188),
    (226,138,43),
    (128,114,250)
)
SLOW_SPEED = 100
FAST_SPEED = 0

# Globals
current_color_index = 0
thread_count = 0
stop = False
speed = SLOW_SPEED

def get_color():
    """ Returns a different color when called """
    global current_color_index
    if current_color_index >= len(COLORS):
        current_color_index = 0
    color = COLORS[current_color_index]
    current_color_index += 1
    return color

# TODO: Add any function(s) you need, if any, here.

def explore(row, col, maze, color):
    global stop, thread_count
    if stop:
        return
    if not maze.can_move_here(row, col):
        return
    maze.move(row, col, color)
    if maze.at_end(row, col):
        stop = True
        return
    moves = maze.get_possible_moves(row, col)
    if not moves:
        return
    children = []
    for nr, nc in moves[1:]:
        if stop:
            break
        new_color = get_color()
        t = threading.Thread(target=explore, args=(nr, nc, maze, new_color))
        t.daemon = True
        thread_count += 1
        t.start()
        children.append(t)
    first_r, first_c = moves[0]
    explore(first_r, first_c, maze, color)
    for t in children:
        t.join()

def solve_find_end(maze):
    """ Finds the end position using threads. Nothing is returned. """
    # When one of the threads finds the end position, stop all of them.
    global stop
    stop = False

    start_r, start_c = maze.get_start_pos()
    initial_color = get_color()
    t0 = threading.Thread(target=explore, args=(start_r, start_c, maze, initial_color))
    t0.daemon = True
    global thread_count
    thread_count += 1
    t0.start()
    t0.join()

def find_end(log, filename, delay):
    """ Do not change this function """

    global thread_count
    global speed

    # create a Screen Object that will contain all of the drawing commands
    screen = Screen(SCREEN_SIZE, SCREEN_SIZE)
    screen.background((255, 255, 0))

    maze = Maze(screen, SCREEN_SIZE, SCREEN_SIZE, filename, delay=delay)

    solve_find_end(maze)

    log.write(f'Number of drawing commands = {screen.get_command_count()}')
    log.write(f'Number of threads created  = {thread_count}')

    done = False
    while not done:
        if screen.play_commands(speed):
            key = cv2.waitKey(0)
            if key == ord('1'):
                speed = SLOW_SPEED
            elif key == ord('2'):
                speed = FAST_SPEED
            elif key == ord('q'):
                exit()
            elif key != ord('p'):
                done = True
        else:
            done = True

def find_ends(log):
    """ Do not change this function """

    files = (
        ('very-small.bmp', True),
        ('very-small-loops.bmp', True),
        ('small.bmp', True),
        ('small-loops.bmp', True),
        ('small-odd.bmp', True),
        ('small-open.bmp', False),
        ('large.bmp', False),
        ('large-loops.bmp', False),
        ('large-squares.bmp', False),
        ('large-open.bmp', False)
    )

    log.write('*' * 40)
    log.write('Part 2')
    for filename, delay in files:
        filename = f'./mazes/{filename}'
        log.write()
        log.write(f'File: {filename}')
        find_end(log, filename, delay)
    log.write('*' * 40)

def main():
    """ Do not change this function """
    sys.setrecursionlimit(5000)
    log = Log(show_terminal=True)
    find_ends(log)

if __name__ == "__main__":
    main()
