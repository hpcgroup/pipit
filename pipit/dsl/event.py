
class Event:
    def __init__(self, timestamp, name, event_type, process, thread):
        self.timestamp = timestamp
        self.name = name
        self.event_type = event_type
        self.process = process
        self.thread = thread

    def to_dict(self):
        return dict(
            timestamp=self.timestamp,
            name=self.name,
            event_type=self.event_type,
            process=self.process,
            thread=self.thread
        )
    
    @staticmethod
    def from_dict(d):
        return Event(d['timestamp'], d['name'], d['event_type'], d['process'], d['thread'])

    def __str__(self):
        return f"Event<'{self.timestamp} {self.event_type} {self.name} {self.process} {self.thread}'>"
    
    def __repr__(self):
        return f"Event<'{self.timestamp} {self.event_type} {self.name} {self.process} {self.thread}'>"