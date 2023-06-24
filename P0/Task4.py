"""
Read file into texts and calls.
It's ok if you don't understand how to read files.
"""
import csv

with open('texts.csv', 'r') as f:
    reader = csv.reader(f)
    texts = list(reader)

with open('calls.csv', 'r') as f:
    reader = csv.reader(f)
    calls = list(reader)

"""
TASK 4:
The telephone company want to identify numbers that might be doing
telephone marketing. Create a set of possible telemarketers:
these are numbers that make outgoing calls but never send texts,
receive texts or receive incoming calls.

Print a message:
"These numbers could be telemarketers: "
<list of numbers>
The list of numbers should be print out one per line in lexicographic order with no duplicates.
"""

set_area_code = set()
for i in range(len(calls)-1):
    if calls[i][0][0:3] == '140':
        set_area_code.add(calls[i][0])

list_area_code = sorted(list(set_area_code))
print("These number could be telemarketers: ")
for i in range(len(list_area_code)):
    print(list_area_code[i])
               
