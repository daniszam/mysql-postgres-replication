from replication.operation import Operation


class Metadata(object):

    def __init__(self, logpos, schema, table, event_time, event: Operation) -> None:
        super().__init__()
        self.logpos = logpos
        self.schema = schema
        self.table = table
        self.event_time = event_time
        self.event = event
