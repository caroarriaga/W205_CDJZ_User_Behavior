#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
calls apache bench for game api file {n} number of times where function call is:
python event_generator.py {n}
if n is not specified, defaults to 100 runs
"""

import random 
import sys
import shlex
import numpy as np
from math import log
import subprocess

# variable lists to randomly select
event_types = ['purchase', 'join_a_guild']
item_names = ['sword', 'shield', 'helmet', 'knife', 'gauntlets']
host_provider_list = ['comcast','att','google','frontier','cox','earthlink']

# get number of loops from first argument when calling function. default to 100
try:
    loops = int(sys.argv[1])
except:
    print('No number of loops specified. Default to 100 loops.')
    loops = 100

curl_template = """\
docker-compose exec mids ab \
-n {num_events} \
-c {concurrent_events} \
{verb} \
-H 'Host: user{user_num}.{host_provider}.com' \
'http://localhost:5000/{event_type}{item_name}'
"""

def build_call():
    event_type = event_types[random.randrange(len(event_types))]
    verb_rand = random.randrange(1,2)
    if verb_rand == 1:
        verb = '-T -p'
    else:
        verb = ''

    if event_type == 'purchase':
        item_name = '/' + item_names[random.randrange(len(item_names))]
    else:
        item_name = ''

    num_events = int(np.ceil(100/random.randrange(1,100)))
    concurrent_events = random.randrange(1,num_events+1)
    user_num = random.randrange(1,100)
    host_provider = host_provider_list[random.randrange(0,len(host_provider_list))]
    
    return curl_template.format(
        num_events = num_events
        , concurrent_events = concurrent_events
        , verb = verb
        , event_type = event_type
        , item_name = item_name
        , user_num = user_num
        , host_provider = host_provider
    )

call_list = [build_call() for x in list(range(loops))]

result_list = []

for cmd in call_list:
    split_cmd = shlex.split(cmd)
    output = subprocess.check_output(split_cmd)
    result_list.append(output)
    
with open('apache_bench_calls.txt', 'w') as filehandler:
    for cmd in call_list:
        filehandler.write('{}'.format(cmd))
print('Printed ab calls to apache_bench_calls.txt')

with open('call_output.txt', 'w') as filehandler:
    for output in result_list:
        filehandler.write('{}\n'.format(output))
print('Printed call outputs to call_output.txt')
