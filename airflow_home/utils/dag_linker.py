from .docker import create_linked_docker_operator


def link_operators(activity_list, tasks):
    """Connects docker operators."""
    depends_on = depends_activities(activity_list)

    if all(depends == 0 for depends in depends_on):
        for i, current in enumerate(tasks):
            if i > 0:
                current.set_upstream(tasks[i - 1])
    else:
        activity_operators = {activity['id']: tasks[idx] for idx, activity in enumerate(activity_list)}
        for activity, task in zip(activity_list, tasks):
            deps_ops = [activity_operators[x] for x in activity.get('dependsOn', [])]
            print(deps_ops)
            task.set_upstream(deps_ops)


def depends_activities(activity_list):
    """Returns list of dependencies."""
    depends_on = []
    for activity in activity_list:
        if 'dependsOn' in activity:
            depends_on.append(activity['dependsOn'])
        else:
            depends_on.append(0)
    return depends_on
