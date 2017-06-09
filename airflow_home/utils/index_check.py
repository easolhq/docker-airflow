def index_check(value, activity_list):
    return next(index for (index, activity) in enumerate(activity_list) if activity["id"] == value)
