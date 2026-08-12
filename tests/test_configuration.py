import json
from pathlib import Path

import pytest
import yaml

from directus_git_sync.api import API
from directus_git_sync.commands import _load_configuration, build_plan
from directus_git_sync.util import export_dir


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value))


def snapshot(tmp_path, with_policy=True):
    dump(tmp_path / 'settings.yaml', {'id': 1, 'project_name': 'FloodNet'})
    dump(tmp_path / 'schema' / '__meta__.yaml', {'directus': '11.9.0', 'sort': []})
    dump(tmp_path / 'policies' / 'engineer-p1.yaml', {
        'id': 'p1', 'name': 'Engineer', 'admin_access': False,
        'app_access': True, 'roles': [], 'users': [],
    }) if with_policy else (tmp_path / 'policies').mkdir()
    dump(tmp_path / 'roles' / 'engineer-r1.yaml', {
        'id': 'r1', 'name': 'Engineer', 'policies': ['p1'],
        'children': [], 'users': [],
    })
    dump(tmp_path / 'permissions' / 'read-1.yaml', {
        'id': 1, 'policy': 'p1', 'action': 'read', 'collection': 'sensors',
    })
    for name in ('flows', 'operations', 'dashboards', 'presets', 'extensions'):
        (tmp_path / name).mkdir()


class FakeAPI(API):
    def __init__(self):
        pass
    def diff_unpacked_schema(self, schema, force=False):
        return None

    def export_settings(self):
        return {'id': 2, 'project_name': 'Old'}

    def export_extensions(self):
        return []

    def json(self, method, route):
        values = {
            '/policies': [{'id': 'admin', 'admin_access': True}, {
                'id': 'p1', 'name': 'Engineer', 'admin_access': False,
                'app_access': True, 'roles': ['r1'], 'users': ['u1'],
            }],
            '/roles': [{'id': 'admin-role', 'policies': ['admin']}, {
                'id': 'r1', 'name': 'Old', 'policies': ['p1'],
                'children': ['child'], 'users': ['u1'],
            }],
            '/permissions': [
                {'id': 1, 'policy': 'p1', 'action': 'read', 'collection': 'sensors'},
                {'id': 2, 'policy': 'admin', 'system': True},
            ],
            '/flows': [], '/operations': [], '/dashboards': [], '/panels': [],
            '/presets': [{'id': 99, 'user': 'u1'}], '/webhooks': [],
        }
        return {'data': values[route]}


def test_diff_items_is_non_mutating_and_ignores_runtime_bindings():
    desired = [{'id': 'one', 'name': 'Engineer', 'users': []}]
    current = [{'id': 'one', 'name': 'Engineer', 'users': ['runtime-user']}]
    assert API('http://example.invalid').diff_items(
        '/roles', desired, existing=current, forbidden_keys=['users']) == {
            'create': [], 'update': [], 'delete': [],
        }
    assert desired[0]['users'] == []
    assert current[0]['users'] == ['runtime-user']


def test_export_dir_renames_existing_object_by_id(tmp_path):
    directory = tmp_path / 'permissions'
    dump(directory / 'None-read-sensors-1.yaml', {
        'id': 1, 'policy': 'p1', 'action': 'read', 'collection': 'sensors',
    })
    export_dir([{
        'id': 1, 'policy': 'p1', 'action': 'read', 'collection': 'sensors',
    }], tmp_path, 'permissions', ['policy', 'action', 'collection', 'id'])
    assert not (directory / 'None-read-sensors-1.yaml').exists()
    assert (directory / 'p1-read-sensors-1.yaml').exists()


def test_load_configuration_rejects_missing_policy(tmp_path):
    snapshot(tmp_path, with_policy=False)
    with pytest.raises(ValueError, match='policies that were not exported: p1'):
        _load_configuration(tmp_path)


def test_build_plan_covers_managed_resources_without_deleting_system_state(tmp_path):
    snapshot(tmp_path)
    result = build_plan(FakeAPI(), tmp_path)
    assert result['settings'] == {
        'project_name': {'current': 'Old', 'desired': 'FloodNet'},
    }
    assert result['resources']['policies'] == {
        'create': [], 'update': [], 'delete': [],
    }
    assert result['resources']['roles'] == {
        'create': [], 'update': ['r1'], 'delete': [],
    }
    assert result['resources']['permissions']['delete'] == []
    assert result['destructive'] is False
    assert result['has_changes'] is True
    json.dumps(result)
