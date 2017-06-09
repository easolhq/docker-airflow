from utils.index_check import index_check


def test_index_check():
    depends_on = [1]
    activity_list = [{"name": "test_1", "id": 1}, {"name": "test_2", "id": 2, "depends_on": [1]}]
    index = index_check(1, activity_list)
    assert index == 0
