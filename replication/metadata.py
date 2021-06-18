from replication.operation import Operation


class Metadata(object):

    def __init__(self, logpos, schema, table, event_time, event: Operation) -> None:
        super().__init__()
        self.logpos = logpos
        self.schema = schema
        self.table = table
        self.event_time = event_time
        self.event = event

    def __str__(self) -> str:
        return 'logpos: {0}, schema: {1}, table: {2}, event_time: {3}, event: {4}'.format(self.logpos, self.schema,
                                                                                          self.table, self.event_time,
                                                                                          self.event)
