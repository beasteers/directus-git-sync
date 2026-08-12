import os
import copy
from ruamel.yaml import YAML
import directus_git_sync as dg
yaml = YAML(typ='safe', pure=True)


def schema_norm(schema):
    schema = copy.deepcopy(dict(schema))
    for field in schema['fields']:
        if field.get('meta'):
            field['meta'].pop('sort', None)
            field['meta'].pop('group', None)
    for collection in schema['collections']:
        if collection.get('meta'):
            collection['meta'].pop('sort', None)
            collection['meta'].pop('group', None)
    schema['collections'] = sorted(schema['collections'], key=lambda x: x['collection'])
    schema['fields'] = sorted(
        schema['fields'], key=lambda x: (x['collection'], x['field']))
    schema['relations'] = sorted(
        schema['relations'],
        key=lambda x: (x['collection'], x['field'], x.get('related_collection') or ''))
    return schema

FNAME = 'tests/schema.yaml'

def test_unpack(tmp_path, fname=FNAME):
    out_dir = str(tmp_path / 'schema')
    with open(fname, "r") as file:
        schema = dict(yaml.load(file))

    d = dg.util.unpack_schema(schema)
    dg.util.export_dir(d, out_dir)
    d2 = dg.util.load_dir(out_dir, as_dict=True)
    assert d == d2

    schema2 = dg.util.pack_schema(d)
    assert schema_norm(schema) == schema_norm(schema2)
