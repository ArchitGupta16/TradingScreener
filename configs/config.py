from omegaconf import OmegaConf

class config:

    def __init__(self):
        conf = OmegaConf.load("query.yaml")
        self.queries = conf.queries
