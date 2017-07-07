"""
Load config objects for DAGs from Mongo.
"""

import os
import ssl

import pymongo


class MongoClient:
    """Wrap PyMongo client for loading workflow config objects."""

    def __init__(self):
        """Instantiate PyMongo client."""
        # Get mongo url.
        mongo_url = os.getenv('MONGO_URL', '')

        # Connect to mongo.
        print('Connecting to mongodb.')
        self.client = pymongo.MongoClient(mongo_url, ssl_cert_reqs=ssl.CERT_NONE)
        self.db = self.client.get_default_database()

    def _build_pipeline(self):
        """Build an aggregation pipeline that populates connections and VPN connections."""
        pipeline = [
            # stage 1: unwind all activities
            {
                "$unwind": {
                    "path": "$activityList",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # stage 2: lookup the connectionConfig for each activity
            {
                "$lookup": {
                    "from": "connectionConfigs",
                    "localField": "activityList.connection",
                    "foreignField": "_id",
                    "as": "activityList.config.connection"
                }
            },
            # stage 3: regroup the activities by workflow
            {
                "$group": {
                    "_id": "$_id",
                    "name": {"$first": "$name"},
                    "path": {"$first": "$path"},
                    "schedule": {"$first": "$schedule"},
                    "pokeInterval": {"$first": "$pokeInterval"},
                    "timeout": {"$first": "$timeout"},
                    "organizationId": {"$first": "$organizationId"},
                    "activityList": {"$push": "$activityList"}
                }
            },
            # stage 4: unwind activityList again since it's a nested array
            {
                "$unwind": {
                    "path": '$activityList',
                    "preserveNullAndEmptyArrays": True
                }
            },
            # stage 5: unwind the connections
            {
                "$unwind": {
                    "path": "$activityList.config.connection",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # stage 6: lookup the vpnConnection for each connection
            {
                "$lookup": {
                    "from": "connectionConfigs",
                    "localField": "activityList.config.connection.vpnConnection",
                    "foreignField": "_id",
                    "as": "activityList.config.connection.vpnConnection"
                }
            },
            # stage 7: unwind the activityList once again
            {
                "$unwind": {
                    "path": '$activityList',
                    "preserveNullAndEmptyArrays": True
                }
            },
            # stage 8: unwind the vpnConnection
            {
                "$unwind": {
                    "path": "$activityList.config.connection.vpnConnection",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # stage 9: regroup the activities by workflow
            {
                "$group": {
                    "_id": '$_id',
                    "name": {"$first": "$name"},
                    "path": {"$first": "$path"},
                    "schedule": {"$first": "$schedule"},
                    "pokeInterval": {"$first": "$pokeInterval"},
                    "timeout": {"$first": "$timeout"},
                    "organizationId": {"$first": "$organizationId"},
                    "activityList": {"$push": "$activityList"}
                }
            },
            # stage 10: lookup the organization for each workflow
            {
                "$lookup": {
                    "from": "organizations",
                    "localField": "organizationId",
                    "foreignField": "_id",
                    "as": "organization"
                }
            },
            # stage 11: unwind the organization
            {
                "$unwind": {
                    "path": "$organization",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # stage 12: unwind the activityList
            {
                "$unwind": {
                    "path": "$activityList",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # stage 13: regroup activities by workflow
            {
                "$group": {
                    "_id": '$_id',
                    "name": {"$first": "$name"},
                    "path": {"$first": "$path"},
                    "schedule": {"$first": "$schedule"},
                    "pokeInterval": {"$first": "$pokeInterval"},
                    "timeout": {"$first": "$timeout"},
                    "organizationId": {"$first": "$organizationId"},
                    "organization": {"$first": "$organization"},
                    "activityList": {"$push": "$activityList"}
                }
            }
        ]
        return pipeline

    def workflow_configs(self):
        """Load configs for Cloud DAGs."""
        return self.db.workflows.aggregate(self._build_pipeline())

    def webhook_configs(self):
        """Load configs for Webhook DAGs."""
        return self.db.webhookConfigs.aggregate(self._build_pipeline())

    def ftp_configs(self):
        """Load configs for FTP DAGs."""
        return self.db.ftpConfigs.aggregate(self._build_pipeline())

    def clickstream_configs(self):
        """Load configs for Clickstream DAGs."""
        # TODO: when v2 comes out will need to add a look up for the connection
        return self.db.integrationConfigs.find({
            'integration': 'amazon-redshift',
            'config.tables': {
                '$exists': True,
            },
        })

    def close(self):
        """Close Mongo connection."""
        self.client.close()


def _load_configs(method_name):
    """Proxy load workflow configs for one type of DAG."""
    mongo = MongoClient()
    workflows = getattr(mongo, method_name)()
    mongo.close()
    return workflows


def load_clickstream_configs():
    """Proxy load clickstream workflow configs."""
    return _load_configs('clickstream_configs')
