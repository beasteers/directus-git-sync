from directus_git_sync.api import sanitize_schema_null_collections


def test_null_meta_collections_drop_fields_and_relations():
    schema = {
        "collections": [
            {"collection": "managed", "meta": {"hidden": False}},
            {"collection": "database_only", "meta": None},
        ],
        "fields": [
            {"collection": "managed", "field": "id"},
            {"collection": "database_only", "field": "id"},
        ],
        "relations": [
            {"collection": "managed", "field": "owner"},
            {"collection": "database_only", "field": "owner"},
        ],
    }

    result = sanitize_schema_null_collections(schema)

    assert [item["collection"] for item in result["collections"]] == ["managed"]
    assert [item["collection"] for item in result["fields"]] == ["managed"]
    assert [item["collection"] for item in result["relations"]] == ["managed"]


def test_fields_for_omitted_database_collections_are_dropped():
    schema = {
        "collections": [{"collection": "managed", "meta": {"hidden": False}}],
        "fields": [
            {"collection": "managed", "field": "id"},
            {"collection": "database_only", "field": "id"},
        ],
        "relations": [{"collection": "database_only", "field": "owner"}],
    }

    result = sanitize_schema_null_collections(schema)

    assert [item["collection"] for item in result["fields"]] == ["managed"]
    assert result["relations"] == []
