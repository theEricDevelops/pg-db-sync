def load_config(file_path):
    import json

    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    
    return config

def get_database_config(config):
    return config.get('database', {})

def get_sync_settings(config):
    return config.get('sync', {})