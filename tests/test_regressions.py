import requests

import pytest

from directus_git_sync.api import API
from directus_git_sync.topo_sort import min_topological_sort
from directus_git_sync.util import dump_data, load_data


def test_txt_round_trip(tmp_path):
    path = tmp_path / 'sample.txt'
    dump_data('hello world', str(path))
    assert load_data(str(path)) == 'hello world'


def test_min_topological_sort_detects_cycle():
    with pytest.raises(ValueError, match='cyclic'):
        min_topological_sort({'A': {'B'}, 'B': {'A'}})


def test_min_topological_sort_groups_parallel_nodes():
    graph = {'B': {'A'}, 'C': {'B'}, 'D': {'B'}}
    assert min_topological_sort(graph, flat=False) == [{'A'}, {'B'}, {'C', 'D'}]
    assert min_topological_sort({'B': {'A'}, 'C': {'B'}}, flat=True) == ['A', 'B', 'C']


def test_json_merges_caller_headers(monkeypatch):
    captured = {}

    class FakeResponse:
        ok = True
        status_code = 200
        content = b'{}'

        def raise_for_status(self):
            pass

        def json(self):
            return {}

    def fake_request(method, url, **kw):
        captured['headers'] = kw['headers']
        return FakeResponse()

    monkeypatch.setattr(requests, 'request', fake_request)
    api = API('http://example.invalid')
    api.headers['Authorization'] = 'Bearer token'
    api.json('GET', '/x', headers={'X-Custom': '1'})
    assert captured['headers'] == {'Authorization': 'Bearer token', 'X-Custom': '1'}


def test_iter_items_honors_limit():
    api = API('http://example.invalid')
    pages = [
        {'data': [{'id': i} for i in range(1, 4)]},
        {'data': [{'id': i} for i in range(4, 7)]},
        {'data': [{'id': i} for i in range(7, 9)]},
        {'data': []},
    ]

    def fake_json(method, route, **kw):
        assert route == '/items/things'
        offset = kw['params']['offset']
        return pages[offset // 3]

    api.json = fake_json
    result = list(api.iter_items('things', batch=3, limit=5))
    assert [item['id'] for batch in result for item in batch] == [1, 2, 3, 4, 5]


def test_apply_does_not_mutate_input_and_ignores_user_updated():
    api = API('http://example.invalid')
    captured = {}

    def fake_json(method, route, **kw):
        if method == 'GET' and route == '/roles':
            return {'data': [{
                'id': 'r1', 'name': 'Old',
                'user_created': 'c', 'user_updated': 'u', 'users': ['runtime'],
            }]}
        if route == '/roles/r1':
            captured['patch'] = kw['json']
            return {'data': {}}
        raise AssertionError(f'unexpected route {route}')

    api.json = fake_json
    items = [{
        'id': 'r1', 'name': 'New',
        'user_created': 'keep', 'user_updated': 'keep', 'users': ['runtime'],
    }]
    api.apply_roles(items, allow_delete=False)

    assert items[0]['user_created'] == 'keep'
    assert items[0]['user_updated'] == 'keep'
    assert items[0]['users'] == ['runtime']
    assert captured['patch']['name'] == 'New'
    assert 'user_created' not in captured['patch']
    assert 'user_updated' not in captured['patch']
    assert 'users' not in captured['patch']


def test_apply_roles_preserves_administrator_and_assigned_roles():
    api = API('http://example.invalid')
    requests = []

    def fake_json(method, route, **kw):
        if method == 'GET' and route == '/roles':
            return {'data': [
                {
                    'id': 'administrator', 'name': 'Administrator',
                    'admin_access': None, 'users': ['admin-user'],
                },
                {
                    'id': 'assigned', 'name': 'Assigned',
                    'admin_access': False, 'users': ['operator-user'],
                },
                {
                    'id': 'unused', 'name': 'Unused',
                    'admin_access': False, 'users': [],
                },
            ]}
        requests.append((method, route, kw))
        return {'data': {}}

    api.json = fake_json
    api.apply_roles([], allow_delete=True)

    assert requests == [('DELETE', '/roles', {'json': ['unused']})]


def test_apply_policies_preserves_builtin_and_assigned_policies():
    api = API('http://example.invalid')
    requests = []

    def fake_json(method, route, **kw):
        if method == 'GET' and route == '/policies':
            return {'data': [
                {
                    'id': 'administrator', 'name': 'Administrator',
                    'admin_access': True, 'roles': ['admin-role'], 'users': [],
                },
                {
                    'id': 'public', 'name': '$t:public_label',
                    'admin_access': False, 'roles': ['public-role'], 'users': [],
                },
                {
                    'id': 'assigned', 'name': 'Assigned',
                    'admin_access': False, 'roles': ['assigned-role'], 'users': [],
                },
                {
                    'id': 'unused', 'name': 'Unused',
                    'admin_access': False, 'roles': [], 'users': [],
                },
            ]}
        requests.append((method, route, kw))
        return {'data': {}}

    api.json = fake_json
    api.apply_policies([], allow_delete=True)

    assert requests == [('DELETE', '/policies', {'json': ['unused']})]


def test_export_extensions_includes_bundle_members():
    api = API('http://example.invalid')
    api.json = lambda method, route: {'data': [
        {'id': 'top', 'bundle': None, 'schema': {'name': 'top', 'version': '1'}},
        {'id': 'bundle1', 'bundle': None, 'schema': {'name': 'bundle1', 'version': '2'}},
        {'id': 'child', 'bundle': 'bundle1', 'schema': {'name': 'child', 'version': '3'}},
    ]}
    result = api.export_extensions()
    assert {item['id'] for item in result} == {'top', 'bundle1', 'child'}
