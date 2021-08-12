import pytest

@pytest.mark.asyncio
async def test_s3_storage(s3_storage):
    path = '/test-745yuwehkds.txt'
    body = b'test-body\n987645123'

    res = await s3_storage.put(path, body)
    assert res.status in {200, 201}

    res = await s3_storage.get(path)
    assert res.status == 200
    read_body = await res.read()
    assert body == read_body

    res = await s3_storage.delete(path)
    assert res.status == 204

    res = await s3_storage.get(path)
    assert res.status == 404
