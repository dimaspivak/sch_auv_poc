#!/usr/bin/env python

import argparse
import logging

import requests

logging.captureWarnings(True)

parser = argparse.ArgumentParser()
parser.add_argument('--name')
parser.add_argument('--age')
parser.add_argument('url')

args = parser.parse_args()

data = dict(name=args.name, age=args.age)
requests.post(args.url, json=data, verify=False, headers={'X-SDC-APPLICATION-ID': 'abc123'})

print(f'Wrote people data (name={args.name}, age={args.age}) to HTTP Server')

