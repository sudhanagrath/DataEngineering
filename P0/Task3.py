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
TASK 3:
(080) is the area code for fixed line telephones in Bangalore.
Fixed line numbers include parentheses, so Bangalore numbers
have the form (080)xxxxxxx.)

Part A: Find all of the area codes and mobile prefixes called by people
in Bangalore. In other words, the calls were initiated by "(080)" area code
to the following area codes and mobile prefixes:
 - Fixed lines start with an area code enclosed in brackets. The area
   codes vary in length but always begin with 0.
 - Mobile numbers have no parentheses, but have a space in the middle
   of the number to help readability. The prefix of a mobile number
   is its first four digits, and they always start with 7, 8 or 9.
 - Telemarketers' numbers have no parentheses or space, but they start
   with the area code 140.

Print the answer as part of a message:
"The numbers called by people in Bangalore have codes:"
 <list of codes>
The list of codes should be print out one per line in lexicographic order with no duplicates.

Part B: What percentage of calls from fixed lines in Bangalore are made
to fixed lines also in Bangalore? In other words, of all the calls made
from a number starting with "(080)", what percentage of these calls
were made to a number also starting with "(080)"?

Print the answer as a part of a message::
"<percentage> percent of calls from fixed lines in Bangalore are calls
to other fixed lines in Bangalore."
The percentage should have 2 decimal digits
"""

# PART A
set_area_code = set()
for i in range(len(calls)-1):
    if calls[i][0][0:5] == '(080)':
        if calls[i][1][0] == '(':
            loc_parenthesis = calls[i][1].find(')')
            fixed_area_code = calls[i][1][1:loc_parenthesis]
            set_area_code.add(fixed_area_code)
        if calls[i][1][0] == '7' or calls[i][1][0] == '8' or calls[i][1][0] == '9':
            mobile_area_code = calls[i][1][0:4]
            set_area_code.add(mobile_area_code)
        if calls[i][1][0:3] == '140':
            telemarket_area_code = calls[i][1][0:3]
            set_area_code.add(telemarket_area_code)

list_area_code = sorted(list(set_area_code))
print("The numbers called by people in Bangalore have codes:")
for i in range(len(list_area_code)-1):
    print(list_area_code[i])

# PART B
from_calls_count = 0
to_calls_count =0
for i in range(len(calls)-1):
    if calls[i][0][0:5] == '(080)':
        from_calls_count = from_calls_count+1
        if calls[i][1][0:5] == '(080)':
            to_calls_count = to_calls_count+1
print('\n')
print(round(to_calls_count/from_calls_count*100,2), "percent of calls from fixed lines in Bangalore are calls to other fixed lines in Banagore.")
            
                

            
    
