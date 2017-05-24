import os
import pymongo
import ssl
from bson.objectid import ObjectId

# Build an aggregation pipeline that populates connections and vpnConnections
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


class MongoClient:
    def __init__(self):
        # Get mongo url.
        mongo_url = os.getenv('MONGO_URL', '')

        self.client = pymongo.MongoClient(mongo_url, ssl_cert_reqs=ssl.CERT_NONE)
        self.db = self.client.get_default_database()

    def workflow_configs(self):
        return self.db.workflows.aggregate(pipeline)

    def webhook_configs(self):
        return self.db.webhookConfigs.aggregate(pipeline)

    def ftp_configs(self):
        return self.db.ftpConfigs.aggregate(pipeline)

    def clickstream_configs(self):
        # TODO: explain
        # TODO: when v2 comes out will need to add a look up for the connection
        return self.db.integrationConfigs.find({'integration': 'amazon-redshift', 'config.tables': {'$exists': True}})

    def close(self):
        self.client.close()
