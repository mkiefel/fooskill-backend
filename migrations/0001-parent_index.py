#!/usr/bin/env python

import re
import json

import redis


def main():
    matcher = re.compile('group:[^:]+:user:[^:]')
    database = redis.Redis(decode_responses=True)
    for key in database.scan_iter():
        if matcher.match(key):
            user = json.loads(database.get(key))
            old_parent_index = user['V0']['parent_index']
            # Remove the group id: 'group:xxx:user:yyy' -> 'yyy'.
            user['V0']['parent_index'] = old_parent_index.split(':')[3]
            database.set(key, json.dumps(user))
            print(user)


if __name__ == '__main__':
    main()
