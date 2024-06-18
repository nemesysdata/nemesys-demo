sch_bronze_stocks = """
    {
        "type": "record",
        "name": "Stock",
        "fields": [
            {"name": "ticker", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "open", "type": "double"},
            {"name": "high", "type": "double"},
            {"name": "low", "type": "double"},
            {"name": "close", "type": "double"},
            {"name": "volume", "type": "long"}
        ]
    }
"""