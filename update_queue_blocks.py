import re
import os
import pickle
from os import environ, path, getcwd
from block_updater import BlockUpdater
import json

MATCH_REGEX = re.compile('\{\{\$(.*)\}\}')

def group_by_to_expr(block_config):
    curr_grp_by = block_config.get('group_by')

    # only update the property if group_by is currently
    # configured. and don't double dip.
    if curr_grp_by and \
       MATCH_REGEX.search(curr_grp_by) is None:
        block_config['group_by'] = "{{$%s}}" % curr_grp_by
        
    new_grp_by = block_config.get('group_by')    
    if new_grp_by and \
       MATCH_REGEX.search(new_grp_by).group(1) == '':
        block_config['group_by'] = ''

    return block_config

def queue_persistence_name(block_data, block_name):
    keys = [k for k in block_data if re.search('queues', k)]
    if len(keys) == 0:
        return
    elif len(keys) == 1:
        k = keys[0]
        block_data['queues'] = block_data[k]
        del block_data[k]
    else:
        print("ERROR: multiple queue sets persisted for {}".format(block_name))
    return block_data

def update_queue_blocks():
    updater = BlockUpdater('Queue', version='1.0',
                           cfg_updates=[group_by_to_expr],
                           persist_updates=[queue_persistence_name])
    updater.update_configs()
    updater.update_persistence()
    updater.dump()

if __name__ == "__main__":
    update_queue_blocks()
