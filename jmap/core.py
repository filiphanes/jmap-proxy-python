class Core:
    capabilityValue = {
        "collationAlgorithms": [
            "i;ascii-numeric",
            "i;ascii-casemap",
            "i;octet"
        ],
        "maxCallsInRequest": 64,
        "maxObjectsInGet": 1000,
        "maxSizeUpload": 250000000,
        "maxConcurrentRequests": 10,
        "maxObjectsInSet": 1000,
        "maxConcurrentUpload": 10,
        "maxSizeRequest": 10000000
    }

    def api_Core_echo(self, **kwargs):
        return kwargs
    
    def api_Blob_copy(self, accountId, **kwargs):
        raise NotImplementedError()
