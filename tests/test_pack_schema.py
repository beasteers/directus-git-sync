import os
from ruamel.yaml import YAML
import directus_git_sync as dg
yaml = YAML(typ='safe', pure=True)


def schema_norm(schema):
    schema = dict(schema)
    schema['collections'] = sorted(schema['collections'], key=lambda x: x['collection'])
    schema['fields'] = sorted(schema['fields'], key=lambda x: x['collection'])
    schema['relations'] = sorted(schema['relations'], key=lambda x: x['collection'])
    return schema

# FNAME = 'tests/schema.yaml'
FNAME = '/opt/gh/mini-floodnet/application/directus/schema.yaml'

def test_unpack(fname=FNAME, out_dir=None):
    out_dir = out_dir or os.path.join(os.path.splitext(fname)[0])
    with open(fname, "r") as file:
        schema = dict(yaml.load(file))

    d = dg.util.unpack_schema(schema)
    dg.util.export_dir(d, out_dir)
    d2 = dg.util.load_dir(out_dir, as_dict=True)
    assert d == d2

    schema2 = dg.util.pack_schema(d)
    assert schema_norm(schema) == schema_norm(schema2)

