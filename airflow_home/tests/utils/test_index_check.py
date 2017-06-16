from utils.index_check import index_check


def test_single_index_check():
    activity_list = [{"name": "test_1", "id": 1}, {"name": "test_2", "id": 2, "depends_on": [1]}]
    index = index_check(1, activity_list)
    assert index == 0


def test_no_index_check():
    activity_list = [{"name": "test_1", "id": 1}, {"name": "test_2", "id": 2}]
    index = index_check(0, activity_list)
    assert index is None


def test_multiple_index_check():
    activity_list = [{"name": "test_1", "id": 1}, {"name": "test_2", "id": 2, "depends_on": [1]},
                     {"name": "test_3", "id": 3, "depends_on": [2]}]
    index = index_check(2, activity_list)
    assert index == 1
