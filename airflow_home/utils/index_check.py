def index_check(value, activity_list):
    """
    Returns index of activity based on id
    :param value: the id of to search for the index
    :type value: integer
    :param activity_list: the list of activities
    :type activity_list: list
    """
    return next((index for (index, activity) in enumerate(activity_list) if activity["id"] == value), None)
