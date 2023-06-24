# Big O Notation for the Project tasks:

## Introduction
This document is about the run time analysis of the 5 programs coded for the project Unscramble the Computer Science problems. This analysis is based on the number of lines in the program written to complete the tasks. i.e. It does not consider reading the two CSV files. If set of lines in the program depend upon the program input, worst case approximation of Big O notation is considered.


## Task0
There are 5 lines in total and none of them depends upon the input.
Big O notation for this task is Ω(0*n + 5) ≈ Ω(1)

## Task1
There are 2 stand alone lines and two for loops each computing two lines of code. If we also consider the time for computing each of the two for loops:
Ω(3*n + 3*n + 2) ≈ Ω(n)


## Task2
There are 3 stand alone lines and 1 for loop iterating to find the maximum of the number in a list. Considering the worst case scenario where the loop has to iterate over the entire list to get the maximum number, the Big O notation is:
Ω(5*n + 3) ≈ Ω(n)

## Task3
There are 3 for loops, 6 stand alone lines and 1 sort function. First loop iterates over 11 lines of code, second iterates over one line, third iterates over 4 lines, considering the computation for each of the for loop also, the big O notation becomes:

Ω(12*n + n + 5*n + n*log(n)) 

where nlog(n) is the order of     sorted function which uses mergesort algorithm. The worst case approximation becomes
 Ω(12*n + n + 5*n + n*log(n)) ≈ Ω(nlog(n))


## Task4

This task uses 2 for loops, one stand alone line, one sorted function. The first for loop iterates over 2 lines, the second iterates over one line. The run time complexity of the sorted() is n*log(n). 

Considering the computation of each of the two loops, the overall complexity is:

Ω(3*n + n + 2 + n*log(n))
Approximating 
Ω(3*n + n + 2 + n*log(n)) ≈ Ω(nlog(n))

