def index_check(value, activity_list):
    """Returns index of activity based on id"""
    return next((index for (index, activity) in enumerate(activity_list) if activity["id"] == value), None)
