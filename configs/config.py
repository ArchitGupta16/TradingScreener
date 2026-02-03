from omegaconf import OmegaConf

class config:

    def __init__(self):
        query_conf = OmegaConf.load("query.yaml")
        prompt_conf = OmegaConf.load("prompts.yaml")
        self.queries = query_conf.queries
        self.prompts = prompt_conf.prompts
