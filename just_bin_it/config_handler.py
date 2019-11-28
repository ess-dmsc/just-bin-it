class ConfigHandler:
    def parse(self, config, current_time=None):
        hist_configs = []

        brokers = config["data_brokers"]
        topics = config["data_topics"]
        start = config["start"] if "start" in config else None
        stop = config["stop"] if "stop" in config else None

        for hist in config["histograms"]:
            hist["data_brokers"] = brokers
            hist["data_topics"] = topics
            hist["start"] = start
            hist["stop"] = stop
            hist_configs.append(hist)

        return hist_configs
