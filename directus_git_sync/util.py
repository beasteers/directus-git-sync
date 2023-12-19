#!/usr/bin/env python3

import os
import re
import csv
import json
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import logging
log = logging.getLogger(__name__)



def load_dir(src_dir):
    if os.path.isfile(src_dir):
        return yaml_load(src_dir)
    return [yaml_load(f) for f in glob.glob(f'{src_dir}/*')]

def dump_each(data, out_dir, name, keys=['name', 'id']):
    for i, d in enumerate(data):
        yaml_dump(d, f'{out_dir}/{name}', '-'.join(f'{d.get(k)}' for k in keys) if keys else f'{i}')



def yaml_load(path):
    # log.debug(f"📖 Reading {path}")
    return yaml.load(open(path), Loader=Loader)

def yaml_dump(data, out_dir, fname):
    '''Write out YAML in a git diff-friendly format'''
    return str_dump(yaml.dump(data), out_dir, fname, ext='.yaml', type_name=type(data).__name__)

def str_dump(data, out_dir, fname, ext='', type_name=None):
    path = os.path.join(out_dir, f'{clean(fname)}{ext}')
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    with open(path, 'w') as f:
        f.write(data)
    log.info(f"💾↓ Wrote {type_name or ''} to {path}")
    return fname


def clean(fname):
    return re.sub(r'\s*[^-\s_A-Za-z0-9.]+\s*', ' ', fname)


# class CSVWriter:
#     def __init__(self, fh):
#         self.fh = fh
#         self.writer = None

#     def write(self, row):
#         if not self.writer:
#             self.writer = csv.DictWriter(self.fh, fieldnames=list(row))
#             self.writer.writeheader()
#         self.writer.writerow(row)


def dump_data(data, file_path):
    """
    Write data to a file in CSV, JSON, or YAML format based on the file extension.

    Parameters:
    - data: The data to be written (list of dictionaries).
    - file_path: The path to the file.

    Returns:
    - None
    """
    file_extension = file_path.split('.')[-1].lower()

    if file_extension == 'csv':
        with open(file_path, 'w', newline='') as csv_file:
            data = iter(data)
            first = next(data, None)
            if first is not None:
                csv_writer = csv.DictWriter(csv_file, fieldnames=list(first))
                csv_writer.writeheader()
                csv_writer.writerow(first)
                csv_writer.writerows(data)
        log.info(f"💾↓ Wrote csv to {file_path}")
    elif file_extension == 'json':
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=2)
        log.info(f"💾↓ Wrote json to {file_path}")
    elif file_extension in ['yaml', 'yml']:
        with open(file_path, 'w') as yaml_file:
            yaml.dump(data, yaml_file, default_flow_style=False)
        log.info(f"💾↓ Wrote yaml to {file_path}")
    elif file_extension in ['txt']:
        with open(file_path, 'w') as f:
            f.write(str(data))
        log.info(f"💾↓ Wrote text to {file_path}")
    else:
        raise ValueError("Unsupported file format. Supported formats: csv, json, yaml/yml")
write_data = dump_data

def load_data(file_path):
    """
    Load data from a file in CSV, JSON, or YAML format based on the file extension.

    Parameters:
    - file_path: The path to the file.

    Returns:
    - Loaded data (list of dictionaries).
    """
    file_extension = file_path.split('.')[-1].lower()

    if file_extension == 'csv':
        log.debug(f"📖 Reading csv {file_path}")
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            return [dict(row) for row in csv_reader]
    elif file_extension == 'json':
        log.debug(f"📖 Reading json {file_path}")
        with open(file_path, 'r') as json_file:
            return json.load(json_file)
    elif file_extension in ['yaml', 'yml']:
        log.debug(f"📖 Reading yaml {file_path}")
        with open(file_path, 'r') as yaml_file:
            return yaml.safe_load(yaml_file)
    elif file_extension in ['txt']:
        log.debug(f"📖 Reading text {file_path}")
        with open(file_path, 'w') as f:
            return f.read()
    else:
        raise ValueError("Unsupported file format. Supported formats: csv, json, yaml/yml")
