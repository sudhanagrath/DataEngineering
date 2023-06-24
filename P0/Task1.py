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
TASK 1:
How many different telephone numbers are there in the records? 
Print a message:
"There are <count> different telephone numbers in the records."
"""

# define a set for the unique phone numbers in the records.
unique_phone_numbers = set()

# iterate for phone numbers in texts file.
for i in range(len(texts)-1):
    unique_phone_numbers.add(texts[i][0])
    unique_phone_numbers.add(texts[i][1])

# iterate for phone numbers in calls file.
for i in range(len(calls)-1):
    unique_phone_numbers.add(calls[i][0])
    unique_phone_numbers.add(calls[i][1])

print("There are", len(unique_phone_numbers), "different telephone numbers in the records.")

    
