from replication.metadata import Metadata


class Batch(object):

    def __init__(self, metadata: Metadata, old_data, new_data) -> None:
        super().__init__()
        self.metadata = metadata
        self.old_data = old_data
        self.new_data = new_data

    def __str__(self) -> str:
        return str(self.metadata) + str(self.old_data) + str(self.new_data)
