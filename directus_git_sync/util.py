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


def export_dir(data, out_dir, name=None, keys=['name', 'id'], ext='yaml'):
    counts = {'unchanged': 0, 'modified': 0, 'new': 0, 'deleted': 0}
    if name is None:
        name = out_dir.rsplit(os.sep, 1)[-1]
    else:
        out_dir = os.path.join(out_dir, name)

    existing = {os.path.splitext(f)[0] for f in os.listdir(out_dir)} if os.path.exists(out_dir) else set()

    if not isinstance(data, dict):
        data = {
            '-'.join(f'{d.get(k)}' for k in keys) if keys else f'{i}': d
            for i, d in enumerate(data)
        }

    for k, d in data.items():
        state = _export_one(d, out_dir, k, ext)
        counts[state] += 1

    for name_i in existing - set(data):
        log.warning("%s :: Removing %s", name, name_i)
        os.remove(get_fname(out_dir, name_i, ext))
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



def load_dir(src_dir, as_dict=False):
    if os.path.isfile(src_dir):
        return load_data(src_dir)
    if as_dict:
        return {os.path.splitext(os.path.basename(f))[0]: load_data(f) for f in glob.glob(f'{src_dir}/*')}
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


def get_key(d, *keys, default=None):
    try:
        for k in keys:
            if isinstance(d, list):
                d = d[int(k)]
            d = d[k]
    except (IndexError, KeyError, TypeError):
        return default
    return d



def pretty_print_schema_diff(diff_data, confirm_delete=False):
    collections = diff_data.get('diff', {}).get('collections', [])
    relations = diff_data.get('diff', {}).get('relations', [])
    fields = diff_data.get('diff', {}).get('fields', [])

    def separate_edits(diffs):
        new_diffs = [{**f, "diff": d} for f in diffs for d in f.get('diff', []) if d.get('kind') == 'N']
        del_diffs = [{**f, "diff": d} for f in diffs for d in f.get('diff', []) if d.get('kind') == 'D']
        other_diffs = [{**f, "diff": [d for d in f.get('diff', []) if d.get('kind') not in ['N', 'D']]} for f in diffs]
        other_diffs = [f for f in other_diffs if f['diff']]
        return new_diffs, del_diffs, other_diffs

    def print_collection(collection_diff):
        diff = collection_diff.get('diff', [])
        sort_change = next((d for d in diff if d.get('path') == ['meta', 'sort']), None)
        group_change = next((d for d in diff if d.get('path') == ['meta', 'group']), None)
        slhs = sort_change.get('lhs') if sort_change else None
        srhs = sort_change.get('rhs') if sort_change else None
        sort_str = (
            'sort'
            + '+'*(sort_change['rhs']-sort_change['lhs'])
            + '-'*(sort_change['lhs']-sort_change['rhs'])
            + f" {sort_change['lhs']} -> {sort_change['rhs']}" 
            if slhs and srhs else '')
        group_str = 'group: ' + f"{group_change.get('lhs')}->{group_change.get('rhs')}" if group_change else ''
        print(color_text(C.BLUE, f"{collection['collection']}:"), group_str, sort_str)

        diff = [d for d in diff if d.get('path') not in [['meta', 'sort'], ['meta', 'group']]]
        for change in diff:
            # kind = change.get('kind')
            # print(" "*2, status_text('modified', f"{kind}:"), change)
            _print_change(change, 2)

    def print_field(field_diff):
        field_name = field_diff.get('field')
        diff = field_diff.get('diff', [])
        diff = [diff] if isinstance(diff, dict) else diff
        # diff = [d for d in diff if d.get('path') != ['meta', 'sort']]]#
        if diff:
            print(" "*5, color_text(C.BLUE, f"{field_name}:"))
        for change in diff:
            _print_change(change, 6)

    def _print_change(change, indent=0):
        kind = change.get('kind')
        path = '.'.join(map(str, change.get('path', [])))
        if kind == "E":
            print(" "*indent, status_text('modified', f"Edit: {path}:"), f"{change['lhs']} -> {change['rhs']}")
        elif kind == "A":
            if change['item']['kind'] == "D":
                print(" "*indent, status_text('deleted', f"Delete: {path}:"), f"{change['index']}: {change['item'].get('lhs')}")
            elif change['item']['kind'] == "N":
                print(" "*indent, status_text('new', f"Insert: {path}:"), f"{change['index']}: {change['item'].get('rhs')}")
            else:
                print(" "*indent, status_text('modified', f"Append: {path}:"), f"{change['index']}: {change['item'].get('lhs')}->{change['item'].get('rhs')}")
        elif kind == "D":
            print(" "*indent, status_text('deleted', f"Delete: {path}:"), change.get('lhs'))
        else:
            print(" "*indent, status_text('modified', f"{kind}:"), change)

    def print_relation(relation_diff):
        collection_name = relation_diff.get('collection')
        field_name = relation_diff.get('field')
        related_collection = relation_diff.get('related_collection')
        for change in relation_diff.get('diff', []):
            kind = change.get('kind')
            path = '.'.join(map(str, change.get('path', [])))
            if kind == "D":
                print(" "*6, status_text('deleted', f"Deleted: {path}:"), f"{collection_name} -> {related_collection}:")
                continue

            print(" "*5, color_text(C.BLUE, collection_name), '->', color_text(C.BLUE, related_collection))
            if kind == "E":
                print(" "*6, status_text('modified', f"Edit: {path}:"), f"{path}: {change['lhs']} -> {change['rhs']}")
            else:
                print(" "*6, status_text('modified', f"{kind}:"), change)

    print(bold(":: DIFF ::"))
    print()

    if not collections and not relations and not fields:
        print("No schema changes detected.")
        return

    changed_collections = [c['collection'] for c in collections]
    other_collections = sorted({f['collection'] for f in fields + relations if f.get('collection') not in changed_collections})

    has_delete = False
    has_changes = False

    new_diffs, del_diffs, other_diffs = separate_edits(collections)
    ignored_diffs = [f for f in del_diffs if not (f['diff'].get('lhs') or {}).get('meta')]
    deleted_diffs = [f for f in del_diffs if (f['diff'].get('lhs') or {}).get('meta')]
    has_delete = has_delete or deleted_diffs
    has_changes = has_changes or new_diffs or deleted_diffs or other_diffs
    if new_diffs:
        print(status_text("new", "New Collections:"), ", ".join([f['collection'] for f in new_diffs]))
    if deleted_diffs:
        print(status_text("deleted", "Delete Collections:"), ", ".join([f['collection'] for f in deleted_diffs]))
    if ignored_diffs:
        print(status_text("none", "Untracked Collections:"), ", ".join([f['collection'] for f in ignored_diffs]))
    if new_diffs or del_diffs:
        print()

    for collection in other_diffs + [{'collection': c} for c in other_collections]:
        c_fields = [f for f in fields if f.get('collection') == collection['collection']]
        c_relations = [r for r in relations if r.get('collection') == collection['collection']]
        new_field_diffs, del_field_diffs, other_field_diffs = separate_edits(c_fields)
        new_diffs, del_diffs, other_diffs = separate_edits(c_relations)
        has_delete = has_delete or del_field_diffs or del_diffs
        has_changes = has_changes or new_field_diffs or del_field_diffs or other_field_diffs or new_diffs or del_diffs or other_diffs
        # if not (new_field_diffs or del_field_diffs or other_field_diffs or new_diffs or del_diffs or other_diffs):
        #     continue

        print_collection(collection)

        if new_field_diffs:
            print("  ", status_text("new", "New Fields:"), ", ".join([f['field'] for f in new_field_diffs]))
        if new_diffs:
            print("  ", status_text("new", "New Relations:"), ", ".join(["{field}(->{related_collection})".format(**f) for f in new_diffs]))
        if del_field_diffs:
            del_field_prop_diffs = [f for f in del_field_diffs if f.get('diff', {}).get('path', [])]
            del_field_diffs = [f for f in del_field_diffs if f not in del_field_prop_diffs]
            if del_field_prop_diffs:
                print("  ", status_text("modified", "Delete Field Properties:"))
                for field in del_field_prop_diffs:
                    print_field(field)
            if del_field_diffs:
                print("  ", status_text("deleted", "Delete Fields:"), ", ".join([f['field'] for f in del_field_diffs]))
        if del_diffs:
            print("  ", status_text("deleted", "Delete Relations:"), ", ".join(["{field}(->{related_collection})".format(**f) for f in del_diffs]))
        if new_diffs or del_diffs or new_field_diffs or del_field_diffs:
            print()

        for field in other_field_diffs:
            print_field(field)
        for relation in other_diffs:
            print_relation(relation)
        if other_field_diffs or other_diffs:
            print()
        # print()

    if not has_changes:
        print(color_text(C.GREEN, "No schema changes detected."))
        print()

    print(bold(":: DIFF ::"))
    print()

    if confirm_delete and has_delete:
        if input("Confirm? y/N: ").lower() != "y":
            raise SystemExit("Aborted.")
    return has_changes


def _get_sort_key(x, default=1e16):
    sort = x.get('meta', {}).get('sort')
    return default if sort is None else sort



def _get_sort_groups(cs):
    top = sorted([c for c in cs if not c.get('meta', {}).get('group')], key=_get_sort_key)
    groups = {}
    for c in cs:
        group = c.get('meta', {}).get('group')
        if group:
            groups.setdefault(group, []).append(c)
    groups = {k: sorted(v, key=_get_sort_key) for k, v in groups.items()}
    return top, groups

def _get_sort_groups_list(cs):
    top, groups = _get_sort_groups(cs)
    top = [c['collection'] for c in top]
    groups = {k: [c['collection'] for c in v] for k, v in groups.items()}
    tree = _nest_groups_list(top, groups)
    # top = [
    #     {c['collection']: [c['collection'] for c in groups[c['collection']]]} 
    #     if c['collection'] in groups else c['collection']
    #     for c in top
    # ]
    return tree


def _nest_groups_list(keys, groups):
    node = []
    for k in keys:
        if k in groups:
            node.append({k: _nest_groups_list(groups[k], groups)})
        else:
            node.append(k)
    return node


def _get_sort_map_inner(c, sort_map, group_map, group=None, i=1):
    if isinstance(c, list):
        for j, v in enumerate(c, 1):
            _get_sort_map_inner(v, sort_map, group_map, group, j)
    elif isinstance(c, dict):
        assert len(c) == 1, "Sort dict must have only one key (e.g. - {Group Name: [collectionA, collectionB]})"
        for ki, vs in c.items():
            _get_sort_map_inner(ki, sort_map, group_map, group, i)
            _get_sort_map_inner(vs, sort_map, group_map, ki, 1)
    elif isinstance(c, str):
        sort_map[c] = i
        group_map[c] = group
    elif c is None:
        pass
    else:
        raise ValueError(f"Invalid sort value: {c}")
    return sort_map, group_map

def _get_sort_map(c_sort):
    sort_map, group_map = _get_sort_map_inner(c_sort, {}, {})
    # for i, c in enumerate(c_sort, 1):
    #     if isinstance(c, dict):
    #         assert len(c) == 1
    #         for k, vs in c.items():
    #             sort_map[k] = i
    #             for j, v in enumerate(vs, 1):
    #                 sort_map[v] = j
    #                 group_map[v] = k
    #     else:
    #         sort_map[c] = i
    return sort_map, group_map


def unpack_schema(schema):
    collections = sorted(schema.get('collections', []), key=_get_sort_key)
    relations = schema.get('relations', [])
    fields = schema.get('fields', [])
    # collection_sort = [d['collection'] for d in collections]
    collection_sort = _get_sort_groups_list(collections)

    # Create file for each collection
    output = {}
    for collection in collections:
        collection_name = collection['collection']
        meta = (collection.get('meta') or {})
        meta.pop('sort', None)
        output[collection_name] = {
            'collection': collection,
            'fields': [f for f in fields if f.get('collection') == collection_name],
            'relations': [r for r in relations if r.get('collection') == collection_name],
        }
    # # Create file for other collections
    # for collection_name in (set(f.get('collection') for f in fields) | set(r.get('collection') for r in relations)) - set(output):
    #     output[collection_name] = {
    #         'fields': [f for f in fields if f.get('collection') == collection_name],
    #         'relations': [r for r in relations if r.get('collection') == collection_name],
    #     }

    # Catch-all for any other fields/relations
    other_fields = [f for f in fields if f.get('collection') not in output]
    other_relations = [r for r in relations if r.get('collection') not in output]
    if other_fields or other_relations:
        output["__unknown_collection__"] = {
            'fields': other_fields,
            'relations': other_relations,
        }

    # Other metadata
    output['__meta__'] = {k: v for k, v in schema.items() if k not in ['collections', 'relations', 'fields']}
    output['__meta__']['sort'] = collection_sort
    return output

def pack_schema(output):
    output = dict(output)
    meta = output.pop('__meta__', {})
    misc = output.pop('__unknown_collection__', {})
    c_sort = meta.pop('sort', [])
    c_sort, c_group = _get_sort_map(c_sort)
    collections = []
    relations = []
    fields = []
    for collection_name, data in output.items():
        if 'collection' in data:
            collections.append(data['collection'])
            if collection_name in c_sort and 'meta' in data['collection']:
                # c_sort.index(collection_name)
                data['collection']['meta']['sort'] = c_sort.get(collection_name)
                data['collection']['meta']['group'] = c_group.get(collection_name)
        else:
            continue
        fields.extend(data['fields'])
        relations.extend(data['relations'])
    return {'collections': collections, 'relations': relations, 'fields': fields, **meta}


# COLLECTION_BOILERPLATE = {
#     'meta': {
#         # 'collection': 'contacts',
#         # 'group': 'Maintanance',
#         # 'sort': 4,
#         'accountability': 'all',
#         'archive_app_filter': True,
#         'archive_field': None,
#         'archive_value': None,
#         'collapse': 'open',
#         'color': None,
#         'display_template': None,
#         'hidden': False,
#         'icon': None,
#         'item_duplication_fields': None,
#         'note': None,
#         'preview_url': None,
#         'singleton': False,
#         'sort_field': None,
#         'translations': None,
#         'unarchive_value': None,
#     }
# }


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

def bold(x, i=True):
    return color_text(C.BOLD, x, i)

def status_text(status, fmt=None, i=True):
    color = COLORS.get(status, status)
    icon = ICONS.get(status,"")
    fmt = fmt or status
    fmt = f'{icon} {i} {fmt}' if i is not True else f'{icon} {fmt}'
    return f'{color}{fmt}{C.END}' if i else fmt