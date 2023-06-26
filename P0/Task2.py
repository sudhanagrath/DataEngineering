"""
Read file into texts and calls.
It's ok if you don't understand how to read files
"""
import csv
with open('texts.csv', 'r') as f:
    reader = csv.reader(f)
    texts = list(reader)

with open('calls.csv', 'r') as f:
    reader = csv.reader(f)
    calls = list(reader)

"""
TASK 2: Which telephone number spent the longest time on the phone
during the period? Don't forget that time spent answering a call is
also time spent on the phone.
Print a message:
"<telephone number> spent the longest time, <total time> seconds, on the phone during 
September 2016.".
"""

# initialise variables for maximum duration and the phone number related to it.
# max_time = 0
# phone_number = ''

# iterate over calls to get the time and phone number.
# for i in range(len(calls)-1):
#    call_time = int(calls[i][3])
#    if call_time > max_time:
#        max_time = call_time
#        phone_number = calls[i][1]

#print(phone_number, 'spent the longest time', max_time, 'on the phone during September 2016.')

dict_call_time = {}

for i in range(len(calls)-1):
    dict_call_time[calls[i][0]] = dict_call_time.get(calls[i][0],0) + int(calls[i][3])

for i in range(len(calls)-1):
    dict_call_time[calls[i][1]] = dict_call_time.get(calls[i][1],0) + int(calls[i][3])

#for key, value in dict_call_time.items():
#    print(key, value)                                                                     

max_key = max(dict_call_time, key = lambda k: dict_call_time[k])
                                                                          
print(max_key , 'spent the longest time', dict_call_time[max_key], 'seconds, on the phone during September 2016')                                                                          

