#!/usr/bin/env python3

import os
import re
import csv
import glob
import json
import yaml
# try:
#     from yaml import CLoader as Loader, CDumper as Dumper
# except ImportError:
#     from yaml import Loader, Dumper

import logging
log = logging.getLogger(__name__.split('.')[0])



def export_one(data, out_dir, name, ext='yaml'):
    state = _export_one(data, out_dir, name, ext)
    log.info("%-11s :: %s.", name.title(), status_text(state))
    return 

def _export_one(data, out_dir, name, ext='yaml'):
    existing = glob.glob(get_fname(out_dir, name, ext))  # TODO: search for other exts
    if existing:
        assert len(existing) == 1
        existing_data = load_data(existing[0])
        if existing_data == data:
            return 'unchanged'
        state = 'modified'
    else:
        state = 'new'
    write_data(data, get_fname(out_dir, name, ext))
    return state


def export_dir(data, out_dir, name, keys=['name', 'id'], ext='yaml'):
    counts = {'unchanged': 0, 'modified': 0, 'new': 0, 'deleted': 0}

    out_dir_i = os.path.join(out_dir, name)
    existing = {os.path.splitext(f)[0] for f in os.listdir(out_dir_i)} if os.path.exists(out_dir_i) else set()
    current = set()
    for i, d in enumerate(data):
        name_i = '-'.join(f'{d.get(k)}' for k in keys) if keys else f'{i}'
        state = _export_one(d, f'{out_dir}/{name}', name_i, ext)
        counts[state] += 1
        current.add(name_i)
    
    for name_i in existing - current:
        log.warning("%s :: Removing %s", name, name_i)
        # os.remove(get_fname(out_dir, name, ext))
        counts['deleted'] += 1

    if not any(counts.values()):
        log.info("%-11s :: 🫥  none.", name.title())
    else:
        log.info(
            "%-11s :: %s. %s. %s. %s.", 
            name.title(), *(
                status_text(k, i=counts[k]) 
                for k in ['new', 'modified', 'deleted', 'unchanged']
            ),
        )



def load_dir(src_dir):
    if os.path.isfile(src_dir):
        return load_data(src_dir)
    return [load_data(f) for f in glob.glob(f'{src_dir}/*')]

# def load_dir(src_dir):
#     if os.path.isfile(src_dir):
#         return yaml_load(src_dir)
#     return [yaml_load(f) for f in glob.glob(f'{src_dir}/*')]

# def dump_each(data, out_dir, name, keys=['name', 'id']):
#     for i, d in enumerate(data):
#         name_i = '-'.join(f'{d.get(k)}' for k in keys) if keys else f'{i}'

#         yaml_dump(d, f'{out_dir}/{name}', name_i)



# def yaml_load(path):
#     # log.debug(f"📖 Reading {path}")
#     return yaml.load(open(path), Loader=Loader)

# def yaml_dump(data, out_dir, fname):
#     '''Write out YAML in a git diff-friendly format'''
#     return str_dump(yaml.dump(data), out_dir, fname, ext='.yaml', type_name=type(data).__name__)

# def str_dump(data, out_dir, fname, ext='', type_name=None):
#     path = get_fname(out_dir, fname, ext)
#     os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
#     with open(path, 'w') as f:
#         f.write(data)
#     log.info(f"💾↓ Wrote {type_name or ''} to {path}")
#     return fname


def get_fname(out_dir, fname, ext):
    return os.path.join(out_dir, f'{clean(fname)}.{ext.lstrip(".")}')

def clean(fname):
    return re.sub(r'\s*[^-\s_A-Za-z0-9.]+\s*', ' ', fname)



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
    # log.info(f"💾↓ pretend write to {file_path}")
    # return

    os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
    if file_extension == 'csv':
        with open(file_path, 'w', newline='') as csv_file:
            # iterator so you can write large csv files
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



def dict_diff(d1, d2):
    missing1 = d2.keys() - d1
    missing2 = d1.keys() - d2
    mismatch = {k for k in d1.keys() & d2 if d1[k] != d2[k]}
    return missing1, missing2, mismatch



class C:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

COLORS = {'new': C.CYAN, 'modified': C.YELLOW, 'deleted': C.RED, 'unchanged': C.GREEN, 'none': C.BLUE}
ICONS = {'new': '🌱', 'modified': '🔧', 'deleted': '🗑 ', 'unchanged': '🌲', 'none': '🫥'}


def color_text(color, x, i=True):
    return f'{color}{x}{C.END}'


def status_text(status, fmt=None, i=True):
    color = COLORS.get(status, status)
    icon = ICONS.get(status,"")
    fmt = fmt or status
    fmt = f'{icon} {i} {fmt}' if i is not True else f'{icon} {fmt}'
    return f'{color}{fmt}{C.END}' if i else fmt