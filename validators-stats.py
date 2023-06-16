#!/usr/bin/env python3

# This script gathers validators stats from the Avascan API and stores them in a CSV file.
# Avascan API: https://api-beta.avascan.info/v2
# Script arguments:
# --network -n: 'mainnet' or 'testnet'
# --status -s: 'active' or 'pending'
# --pages -p: number of pages to fetch (default: 1)
# --output-file -o: 'output/validators-stats.csv' (default)

import requests
import argparse
import csv
import os

AVALANCHE_PRIMARY_NETWORK_ID = '11111111111111111111111111111111LpoYY'
# API endpoint and path
API_ENDPOINT = 'https://api-beta.avascan.info'
API_PATH = 'v2/network/{}/staking/validations'
# Sample output
# {
#   "items": [
#     {
#       "nodeId": "NodeID-H1yZ17sZyZe12kT52ywW3QnVBySqUHLET",
#       "subnetId": "11111111111111111111111111111111LpoYY",
#       "beneficiaries": [
#         "P-avax16ujlw4nzu2rpjk74naxux9cllcagu8mq0vp95a"
#       ],
#       "startTime": "2023-05-15T20:34:15.000Z",
#       "endTime": "2023-06-17T19:00:56.000Z",
#       "assetId": "FvwEAhmxKfeiG8SnEvq42hc6whRyY3EFYAvebMqDNDGCgxN5Z",
#       "stake": {
#         "fromSelf": "2000000000000",
#         "fromDelegations": "0",
#         "total": "2000000000000",
#         "networkShare": 0.0000075919547189551675
#       },
#       "rewards": {
#         "fromSelf": "12465776252",
#         "fromDelegations": "0",
#         "total": "12465776252"
#       },
#       "delegations": {
#         "count": 0,
#         "delegationFee": 0.02,
#         "maxYield": 0.00025915155181999996,
#         "availableDelegationCapacity": "8000000000000",
#         "totalDelegationCapacity": "10000000000000",
#         "grossDelegationReward": "0",
#         "netDelegationReward": "0"
#       },
#       "node": {
#         "avgUptime": 0.9965721428571429,
#         "responsiveness": {
#           "checksCount": 14,
#           "positiveChecksCount": 14
#         },
#         "version": "avalanche/1.10.1",
#         "ip": "173.70.63.179",
#         "isp": "Verizon",
#         "location": {
#           "city": "Cliffside Park",
#           "country": "United States"
#         }
#       }
#     }
#   ],
#   "link": {
#     "next": "/v2/network/mainnet/staking/validations?status=active&next=ec834b0578d430a0a4293f290678413c0592076ea72a7223d7ed86c98bb88a77d0a1ec29fe21edcacff8c1e31530661120a98f76",
#     "nextToken": "ec834b0578d430a0a4293f290678413c0592076ea72a7223d7ed86c98bb88a77d0a1ec29fe21edcacff8c1e31530661120a98f76"
#   }
# }


# Parse arguments
parser = argparse.ArgumentParser(
    description='Gather validators stats from the Avascan API and store them in a CSV file.'
)
parser.add_argument(
    '--network',
    '-n',
    type=str,
    choices=['mainnet', 'testnet'],
    default='mainnet',
    help='Network: mainnet or testnet',
)
parser.add_argument(
    '--status',
    '-s',
    type=str,
    choices=['active', 'pending'],
    default='active',
    help='Status: active or pending',
)
parser.add_argument(
    '--pages',
    '-p',
    type=int,
    default=1,
    help='Number of pages to fetch: 1 (default). -1 for all pages.',
)
parser.add_argument(
    '--output-file',
    '-o',
    type=str,
    default='output/validators-stats.csv',
    help='Output file: output/validators-stats.csv (default)',
)

args = parser.parse_args()

# Get validators stats
validators_stats = []

print('Fetching validators stats...')
print('Network: {}'.format(args.network))
print('Status: {}'.format(args.status))
print('Pages: {}'.format(args.pages))

next_token = ''
page = 1
while page <= args.pages or args.pages == -1:
    url = '{}/{}'.format(API_ENDPOINT, API_PATH.format(args.network))

    if page == 1:
        r = requests.get(
            url,
            params={'status': args.status, 'subnetIds': AVALANCHE_PRIMARY_NETWORK_ID},
        )
        print('API call: {}'.format(r.url))
    else:
        r = requests.get(
            url,
            params={
                'status': args.status,
                'subnetIds': AVALANCHE_PRIMARY_NETWORK_ID,
                'next': next_token,
            },
        )

    if r.status_code != 200:
        print('Error: {}'.format(r.text))
        exit(1)

    print('Page {}/{} fetched: {}'.format(page, args.pages, len(r.json()['items'])))

    validators_stats += r.json()['items']

    if 'nextToken' not in r.json()['link']:
        break
    next_token = r.json()['link']['nextToken']

    page += 1

print('Validators stats fetched: {}'.format(len(validators_stats)))

# Flatten validators stats
print('Flattening validators stats...')
validators_stats_flat = []
for v in validators_stats:
    validators_stats_flat.append(
        {
            'nodeId': v['nodeId'],
            'subnetId': v['subnetId'],
            'name': v.get('name', ''),
            'manager': v.get('manager', ''),
            'icon': v.get('icon', ''),
            'beneficiaries': v['beneficiaries'],
            'startTime': v['startTime'],
            'endTime': v['endTime'],
            'assetId': v['assetId'],
            'stake': v['stake']['total'],
            'stakeFromSelf': v['stake']['fromSelf'],
            'stakeFromDelegations': v['stake']['fromDelegations'],
            'networkShare': v['stake']['networkShare'],
            'rewards': v['rewards']['total'],
            'rewardsFromSelf': v['rewards']['fromSelf'],
            'rewardsFromDelegations': v['rewards']['fromDelegations'],
            'delegations': v['delegations']['count'],
            'delegationFee': v['delegations']['delegationFee'],
            'delegationsMaxYield': v['delegations']['maxYield'],
            'availableDelegationCapacity': v['delegations'][
                'availableDelegationCapacity'
            ],
            'totalDelegationCapacity': v['delegations']['totalDelegationCapacity'],
            'grossDelegationReward': v['delegations']['grossDelegationReward'],
            'netDelegationReward': v['delegations']['netDelegationReward'],
            'avgUptime': v['node']['avgUptime'],
            'responsivenessChecksCount': v['node']['responsiveness']['checksCount'],
            'responsivenessPositiveChecksCount': v['node']['responsiveness'][
                'positiveChecksCount'
            ],
            'version': v['node'].get('version', ''),
            'ip': v['node'].get('ip', ''),
            'isp': v['node'].get('isp', ''),
            'city': v['node']['location'].get('city', ''),
            'country': v['node']['location'].get('country', ''),
        }
    )

# Write validators stats to CSV file
print(f'Writing validators stats to {args.output_file}...')
# Create output directory if it doesn't exist
os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
with open(args.output_file, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=validators_stats_flat[0].keys())
    writer.writeheader()
    for v in validators_stats_flat:
        writer.writerow(v)
