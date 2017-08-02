# docker-airflow

[![CircleCI](https://circleci.com/gh/astronomerio/docker-airflow.svg?style=svg)](https://circleci.com/gh/astronomerio/docker-airflow)

Airflow docker container

## Overriding start_date per DAG
Inside your workflow document, define `workflow.default_args` like so:
```javascript
{
    "name": "my_dag",
    "activityList": [],
    "default_args": {
        "start_date": ISODate("2017-08-01T00:00:00.000+0000")
    }
}
```

## Clickstream DAGs
Clickstream DAGs are dynamically generated using information from the `clickstreamData` Mongo collection:
- one DAG is generated for each record in the collection, corresponding to a separate app
- each DAG has two branches:
  - the first branch processes data in the S3 app directory corresponding to the execution time of the job (events that have just been processed by the streaming service), and copies *standard* events (`page`, `track`, `identify`, `group`, `screen`)
  - the second branch processes data in the second most recent app directory, and copies *dynamic* tracking events (these are dynamically populated from a Mongo app record, using the `tables` property)
- the lag for the second branch has to do with making sure that Airflow has the time to update a DAG definition based on updates in Mongo, before a stale DAG run is generated (leading to possibly missing a whole set of events from the batch that first saw events of that specific type)
- appId, event type, timedelta (0 or 15m) and redshift credentials are passed on to the copy task image via configuration
- each event type has a separate copy task
- all copy tasks are prefixed by a sensor, so although DAG covers all possible events, we only run copy for existing data
- each major branch is also prefixed by a sensor
- all copy tasks are associated to an Airflow pool (this can restrict more than X number of copy tasks to run in parallel, to optimize Redshift copy efficiency); this needs to be pre-created

## Code Quality

To lint, run pycodestyle (formerly pep8) within container:

```
make lint
```

## Tests

Run nose2 at the project root:

```
make test
```
