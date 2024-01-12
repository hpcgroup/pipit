# Class to store individual events and info on their matching events
class Event:
    def __init__(self, event_name, event_type, event_id, time, process, matching_id = -1):
        self.event_name = event_name
        self.event_type = event_type
        self.event_id = event_id
        self.event_time = time
        self.start_time = time
        self.end_time = time
        self.process = process 

        # Lamport Ordering information per event
        self.prev_event = None
        self.next_event = None

        # For now just assumed that there is one matching event
        self.matching_event_id = matching_id
        self.matching_event = None

        self.partition = None

        # TODO: We should add a leap id to each event

        # Stride and Step Information
        self.stride = None
        self.step = None
        self.lateness = 0
        self.diff_latenss = 0

    def add_start_time(self, time):
        self.start_time = time

    def add_end_time(self, time):
        self.end_time = time

    def add_next_event(self, e):
        self.next_event = e

    def get_next_event(self):
        return self.next_event

    def add_prev_event(self, e):
        self.prev_event = e

    def get_prev_event(self):
        return self.prev_event

    def add_matching_event(self, e):
        self.matching_event = e
        self.matching_event_id = e.event_id

    def has_matching_event(self):
        return self.matching_event != None

    def get_matching_event(self):
        return self.matching_event

    def add_partition(self, p):
        self.partition = p

    def get_partition(self):
        return self.partition

    def __hash__(self) -> int:
        return self.event_id
    