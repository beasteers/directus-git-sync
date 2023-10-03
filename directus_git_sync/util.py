#!/usr/bin/env python3

import os
import re
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
    log.debug(f"📖 Reading {path}")
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
