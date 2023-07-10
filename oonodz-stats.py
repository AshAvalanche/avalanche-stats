from web3 import Web3
from web3.middleware import geth_poa_middleware

import json
import pandas as pd

RPC = 'https://api.avax.network/ext/bc/C/rpc'
CONTRACT_ADDRESS = '0x59a90cD4fa3f6F9544fb26EEeE913a35d6E7772e'
CONTRACT_CREATION_BLOCK = 28953227
CONTRACT_EVENTS_START_BLOCK = CONTRACT_CREATION_BLOCK
# CONTRACT_EVENTS_START_BLOCK = 32166124
BLOCKS_PER_QUERY = 2048 # Avalanche Mainnet rate limit is 2,048 blocks per query

# Web3 to Avalanche Mainnet
w3 = Web3(Web3.HTTPProvider(RPC))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

# Open ABI
with open("files/Subscriptions.json") as f:
    info_json = json.load(f)
ABI = info_json["abi"]

# Load contract
contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ABI)

# Get current last block
current_height = w3.eth.get_block('latest')['number']

# Get all event types
# all_events_filter = [
#   print(event)
#   for event in contract.events
# ]
# print(all_events_filter)

# Get all "NewSubscription" and "StopSubscription" events
new_subscriptions_events = []
stop_subscriptions_events = []
i = CONTRACT_EVENTS_START_BLOCK
while i < current_height:
    start_block = i
    # Do not go over current height
    end_block = i + BLOCKS_PER_QUERY - 1 if i + BLOCKS_PER_QUERY - 1 < current_height else current_height
    print("Processing events from block " + str(i) + " to " + str(end_block) + "...")
    # Get "NewSubscription" events in the range
    new_subscriptions_events_range = contract.events.NewSubscription.get_logs(fromBlock=i, toBlock=end_block)
    for new_subscriptions_event in new_subscriptions_events_range:
        new_subscriptions_events.append(new_subscriptions_event)
    # Get "StopSubscription" events in the range
    stop_subscriptions_events_range = contract.events.StopSubscription.get_logs(fromBlock=i, toBlock=end_block)
    for stop_subscriptions_event in stop_subscriptions_events_range:
        stop_subscriptions_events.append(stop_subscriptions_event)
    i += BLOCKS_PER_QUERY

# Convert lists of AttributeDict to list of dict while also converting nested AttributeDict to dict
new_subscriptions_events = [dict(new_subscriptions_event) for new_subscriptions_event in new_subscriptions_events]
for new_subscriptions_event in new_subscriptions_events:
    new_subscriptions_event['args'] = dict(new_subscriptions_event['args'])
    # convert transactionHash and blockHash to hex
    new_subscriptions_event['transactionHash'] = new_subscriptions_event['transactionHash'].hex()
    new_subscriptions_event['blockHash'] = new_subscriptions_event['blockHash'].hex()
stop_subscriptions_events = [dict(stop_subscriptions_event) for stop_subscriptions_event in stop_subscriptions_events]
for stop_subscriptions_event in stop_subscriptions_events:
    stop_subscriptions_event['args'] = dict(stop_subscriptions_event['args'])
    stop_subscriptions_event['transactionHash'] = stop_subscriptions_event['transactionHash'].hex()
    stop_subscriptions_event['blockHash'] = stop_subscriptions_event['blockHash'].hex()

# Create dataframe
all_events = new_subscriptions_events + stop_subscriptions_events
df = pd.json_normalize(all_events)

# Export to CSV
df.to_csv('oonodz-stats.csv', index=False)
