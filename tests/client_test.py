from dateutil.parser import parse


def test_client_setup(client):
    assert client.is_connected


def test_client_query_database(client):
    response = client.query_database(
        "xc_pos_rel_type_hist",
        10,
        "pos_rel_type_id",
        0,
        "MODIFIED_DATE",
        "1970-09-28T00:45:22Z",
    )

    assert len(response) == 10

    for row in response:
        assert "POS_REL_TYPE_ID" in row
        assert "OBJECT_ID" in row
        assert "VERSION" in row
        assert "NAME" in row
        assert "DESCR" in row
        assert "IS_ACTIVE" in row
        assert "CREATED_DATE" in row
        assert parse(row["CREATED_DATE"])
        assert "CREATED_BY_ID" in row
        assert "CREATED_BY_NAME" in row
        assert "MODIFIED_DATE" in row
        assert parse(row["MODIFIED_DATE"])
        assert "MODIFIED_BY_ID" in row
        assert "MODIFIED_BY_NAME" in row
        assert "EFFECTIVE_START_DATE" in row
        assert parse(row["EFFECTIVE_START_DATE"])
        assert "EFFECTIVE_END_DATE" in row
        assert parse(row["EFFECTIVE_END_DATE"])
        assert "IS_MASTER" in row
