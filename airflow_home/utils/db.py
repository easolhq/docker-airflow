import os
import pymongo
import ssl


class MongoClient:
    """MongoDB connection wrapper."""

    def __init__(self):
        """Initialize Mongo and store default database."""
        # Get mongo url.
        mongo_url = os.getenv('MONGO_URL', '')

        # Connect to mongo.
        print('Connecting to mongodb.')
        self.client = pymongo.MongoClient(mongo_url, ssl_cert_reqs=ssl.CERT_NONE)
        self.db = self.client.get_default_database()
        self.pipeline = self._build_pipeline()

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
                    "default_args": {"$first": "$default_args"},
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
                    "default_args": {"$first": "$default_args"},
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
                    "default_args": {"$first": "$default_args"},
                    "organizationId": {"$first": "$organizationId"},
                    "organization": {"$first": "$organization"},
                    "activityList": {"$push": "$activityList"}
                }
            }
        ]
        return pipeline

    def workflow_configs(self):
        """Fetch config objects for Cloud DAGs."""
        return self.db.workflows.aggregate(self.pipeline)

    def webhook_configs(self):
        """Fetch config objects for webhook DAGs."""
        return self.db.webhookConfigs.aggregate(self.pipeline)

    def ftp_configs(self):
        """Fetch config objects for FTP DAGs."""
        return self.db.ftpConfigs.aggregate(self.pipeline)

    def clickstream_configs(self):
        """Fetch config objects for Clickstream DAGs."""
        # integration_configs = self.db.integrationConfigs.find({
        #     'integration': 'amazon-redshift',
        #     'config.tables': {
        #         '$exists': True,
        #     },
        # })
        # return integration_configs
        pipeline = [
            {'$match': {
                'code': 'amazon-redshift',
                'config.schema': {
                    '$exists': True,
                    '$ne': '',
                },
                'config.db': {
                    '$exists': True,
                    '$ne': '',
                },
                "config.tables": {
                    '$exists': True,
                    '$ne': [],
                },
                'enabled': True,
            }},

            {'$lookup': {
                'from': 'connectionConfigs',
                'localField': 'connection',
                'foreignField': '_id',
                'as': 'connection',
            }},

            # {'$group': {
            #     '_id': '$_id',
            #     'connection': {'$first': '$connection'},
            # }},

            # {'$unwind', {
            #     'path': '$connection',
            #     # 'preserveNullAndEmptyArrays': True,
            # }},

            # {'$lookup': {
            #     'from': 'connections',
            #     'localField': 'connection.connectionPrototype',
            #     'foreignField': '_id',
            #     'as': 'connection.connectionPrototype',
            # }},

            # TODO: figure out how to flatten connection first then uncomment
            # {'$lookup': {
            #     'from': 'organizations',
            #     'localField': '$connection.0.organization',
            #     'foreignField': '_id',
            #     'as': '$connection.0.organization',
            # }},

        ]
        items = self.db.clickstreamConfigs.aggregate(pipeline)
        # list(items).__len__()
        # x = list(items)
        # pprint(x)
        return items


    def close(self):
        """Close Mongo client."""
        self.client.close()
