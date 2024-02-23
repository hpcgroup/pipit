class Event:
    def __init__(self, timestamp, name, event_type, **kwargs):
        self.timestamp = timestamp
        self.name = name
        self.event_type = event_type
        self.kwargs = kwargs

    def to_dict(self):
        return dict(
            timestamp=self.timestamp,
            name=self.name,
            event_type=self.event_type,
            **self.kwargs,
        )

    @staticmethod
    def from_dict(d):
        return Event(
            d["timestamp"],
            d["name"],
            d["event_type"],
            **{
                k: v
                for k, v in d.items()
                if k not in ["timestamp", "name", "event_type"]
            },
        )

    def __str__(self):
        return f"{self.timestamp} {self.name} {self.event_type} {self.kwargs}"

    def __repr__(self):
        return self.__str__()
