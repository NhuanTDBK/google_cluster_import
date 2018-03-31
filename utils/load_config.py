import yaml

def load_config(file_path="config.yml"):
    config = yaml.load(open(file_path))
    return config