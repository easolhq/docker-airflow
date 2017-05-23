import datetime

from marshmallow import Schema, fields, post_load
from marshmallow.validate import Range


class RedshiftConfig(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.created_at = datetime.datetime.now()
        self.timedelta = 0

    def __repr__(self):
        return '<Redshift(host={self.host!r})>'.format(self=self)


class RedshiftConfigSchema(Schema):
    workflow_id = fields.Str(required=True, load_from='AppId', dump_to='AppId')
    host = fields.Str(required=True)
    port = fields.Integer(required=True, validate=Range(min=0, max=65535))
    timedelta = fields.Integer(dump_only=True)

    @post_load
    def make_redshift(self, data):
        return Redshift(**data)


class ClickstreamConfig(object):
    pass


class ClickstreamConfigSchema(Schema):
    temp_bucket = fields.Str(required=True)
    redshift = fields.Nested(RedshiftSchema, required=True)


# def load_redshift_schema(data):
#     """Load redshift config and validate schema."""
#     rs = RedshiftConfigSchema()
#     result = rs.load(data)
#     if result.errors:
#         print('errors!')
#         print(result.errors)
#     print(result.data)
#     return result


def main():
    redshift_data = {
        'AppId': '1234',
        'host': 'myhost',
        'port': '123'
    }

    clickstream_data = {
        'temp_bucket': 'abc',
        'config': redshift_data
    }

    ccs = ClickstreamConfigSchema()
    cc, errors = ccs.load(clickstream_data)
    if errors:
        print('errors!', errors)
    print('cc:', cc)

    # dump nested clickstream config
    cc_dump_data = ccs.dump(cc)
    print('cc_dump_data:', cc_dump_data)


if __name__ == '__main__':
    main()
