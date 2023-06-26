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

# set_area_code = set()
# for i in range(len(calls)-1):
  #  if calls[i][0][0:3] == '140':
  #      set_area_code.add(calls[i][0])

# list_area_code = sorted(list(set_area_code))
# print("These number could be telemarketers: ")
# for i in range(len(list_area_code)):
#    print(list_area_code[i])
               
set_telemarket_numbers = set()
set_non_telemarket_numbers = set()

for i in range(len(calls)-1):
    set_telemarket_numbers.add(calls[i][0])
    set_non_telemarket_numbers.add(calls[i][1])

for i in range(len(texts)-1):
    set_non_telemarket_numbers.add(texts[i][0])
    set_non_telemarket_numbers.add(texts[i][1])

set_possible_telemarket_numbers = set_telemarket_numbers.difference(set_non_telemarket_numbers)

list_telemarket_numbers = sorted(list(set_possible_telemarket_numbers))
print("These number could be telemarketers: ")
for i in range(len(list_telemarket_numbers)-1):
    print(list_telemarket_numbers[i])
               
    
