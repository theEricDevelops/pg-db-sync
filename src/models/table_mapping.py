class TableMapping:
    def __init__(self, mapping_config):
        self.mapping_config = mapping_config

    def get_mapping(self):
        return self.mapping_config

    def get_source_table(self):
        return self.mapping_config.get("source_table")

    def get_target_table(self):
        return self.mapping_config.get("target_table")

    def get_column_mapping(self):
        return self.mapping_config.get("column_mapping", {})