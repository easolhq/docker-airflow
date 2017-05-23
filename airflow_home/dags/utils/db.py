import os
import pymongo
import ssl

class MongoClient:
    """
    TODO
    """

    def __init__(self):
        """
        TODO
        """
        # Get mongo url.
        mongo_url = os.getenv('MONGO_URL', '')

        # Connect to mongo.
        print('Connecting to mongodb.')
        self.client = pymongo.MongoClient(mongo_url, ssl_cert_reqs=ssl.CERT_NONE)
        self.db = self.client.get_default_database()
        self.pipeline = self._build_pipeline()

    def _build_pipeline(self):
        """
        Build an aggregation pipeline that populates connections and VPN connections.
        """
        return [
            {
                "$unwind": "$activityList"
            },
            {
                "$lookup": {
                    "from": "connectionConfigs",
                    "localField": "activityList.connection",
                    "foreignField": "_id",
                    "as": "activityList.config.connection"
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "name": {"$first": "$name"},
                    "path": {"$first": "$path"},
                    "schedule": {"$first": "$schedule"},
                    "pokeInterval": {"$first": "$pokeInterval"},
                    "timeout": {"$first": "$timeout"},
                    "accountId": {"$first": "$accountId"},
                    "activityList": {"$push": "$activityList"}
                }
            },
            {
                "$unwind": '$activityList'
            },
            {
                "$unwind": {
                    "path": "$activityList.config.connection",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "connectionConfigs",
                    "localField": "activityList.config.connection.vpnConnection",
                    "foreignField": "_id",
                    "as": "activityList.config.connection.vpnConnection"
                }
            },
            {
                "$unwind": '$activityList'
            },
            {
                "$unwind": {
                    "path": "$activityList.config.connection.vpnConnection",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": '$_id',
                    "name": {"$first": "$name"},
                    "path": {"$first": "$path"},
                    "schedule": {"$first": "$schedule"},
                    "pokeInterval": {"$first": "$pokeInterval"},
                    "timeout": {"$first": "$timeout"},
                    "accountId": {"$first": "$accountId"},
                    "activityList": {"$push": "$activityList"}
                }
            }
        ]

    def workflow_configs(self):
        """
        TODO
        """
        return self.db.workflows.aggregate(self.pipeline)

    def webhook_configs(self):
        """
        TODO
        """
        return self.db.webhookConfigs.aggregate(self.pipeline)

    def ftp_configs(self):
        """
        TODO
        """
        return self.db.ftpConfigs.aggregate(self.pipeline)

    def clickstream_configs(self):
        """
        TODO
        """
        # TODO: explain
        return self.db.clickstreamData.find({})

    def close(self):
        """
        TODO
        """
        self.client.close()
