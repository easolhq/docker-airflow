from .docker import create_linked_docker_operator
from .index_check import index_check


def link_operators(activity_list, tasks, upstream=None):
    """Connects docker operators."""
    depends_on = depends_activities(activity_list)

    check_upstream(upstream, tasks, depends_on)

    if all(depends == 0 for depends in depends_on):
        for i, current in enumerate(tasks):
            if i > 0:
                current.set_upstream(tasks[i - 1])
    else:
        used_activities = []
        for i, depend in enumerate(depends_on):
            if depend != 0:
                set_activities_upstream(depend, activity_list, tasks, used_activities, i)


def check_upstream(upstream, tasks, depends_on):
    """Checks for pre created upstream docker operators."""
    if upstream:
        for i, current in enumerate(tasks):
            if depends_on[i] == 0:
                current.set_upstream(upstream)


def depends_activities(activity_list):
    """Returns list of dependencies."""
    depends_on = []
    for activity in activity_list:
        if 'dependsOn' in activity:
            depends_on.append(activity['dependsOn'])
        else:
            depends_on.append(0)
    return depends_on


def set_activities_upstream(depend_activity, activity_list, tasks, used_activities, outer_index):
    """Links docker operators based off of dependencies."""
    for ind, id_val in enumerate(depend_activity):
        index = index_check(id_val, activity_list)
        if [outer_index, index] not in used_activities:
            tasks[outer_index].set_upstream(tasks[index])
            used_activities.append([outer_index, index])
