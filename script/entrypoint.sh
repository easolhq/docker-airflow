#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $@ != *"airflow"* ]] && [[ $@ != "" ]]; then
    exec $@
fi

: ${AIRFLOW_ROLE:=""}

case "$AIRFLOW_ROLE" in
  webserver|scheduler|logserver)
    ;;
  *)
    echo "AIRFLOW_ROLE (webserver, scheduler, logserver) is not set or has an invalid value"
    echo "Exiting..."
    exit 1
    ;;
esac

# Configure airflow with postgres connection string.
if [ -v AIRFLOW_POSTGRES_HOST ] && [ -v AIRFLOW_POSTGRES_PORT ] && [ -v AIRFLOW_POSTGRES_USER ] && [ -v AIRFLOW_POSTGRES_PASSWORD ] && [ -v AIRFLOW_POSTGRES_DATABASE ]; then
    CONN="postgresql://$AIRFLOW_POSTGRES_USER:$AIRFLOW_POSTGRES_PASSWORD@$AIRFLOW_POSTGRES_HOST:$AIRFLOW_POSTGRES_PORT/$AIRFLOW_POSTGRES_DATABASE"
    echo "Setting AIRFLOW__CORE__SQL_ALCHEMY_CONN=${CONN}"
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$CONN
fi

if [ -v AIRFLOW__CORE__SQL_ALCHEMY_CONN ]; then
    # Wait for postgres then init the db.
    if [[ $AIRFLOW_ROLE == "webserver" ]] || [[ $AIRFLOW_ROLE == "scheduler" ]]; then
        HOST=`echo $AIRFLOW__CORE__SQL_ALCHEMY_CONN | awk -F@ '{print $2}' | awk -F/ '{print $1}'`
        echo "Waiting for host: ${HOST}"
        ${DIR}/wait-for-it.sh ${HOST}

        # Ensure db initialized.
        if [[ $AIRFLOW_ROLE == "webserver" ]] || [[ $AIRFLOW_ROLE == "scheduler" ]]; then
            echo "Initializing airflow postgres db..."
            airflow initdb
        fi

        echo "Ensuring database..."
        sleep 5
    fi
fi

# Run the `airflow` command.
echo "Executing: $@"
exec $@
