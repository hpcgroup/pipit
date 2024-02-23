class Event:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def to_dict(self):
        return self.kwargs

    def __str__(self):
        return f":Event   {self.kwargs.__str__()}"

    def __repr__(self):
        return self.__str__()

    def __getattr__(self, name):
        return self.kwargs[name]
