from replication.metadata import Metadata


class Batch(object):

    def __init__(self, metadata: Metadata, old_data, new_data) -> None:
        super().__init__()
        self.metadata = metadata
        self.old_data = old_data
        self.new_data = new_data
