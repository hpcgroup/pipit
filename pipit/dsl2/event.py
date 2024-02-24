class Event:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def to_dict(self) -> dict:
        return self.kwargs

    def __str__(self) -> str:
        return (
            "Event"
            + f"({', '.join([f'{k}={v.__repr__()}' for k, v in self.kwargs.items()])})"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def __getattr__(self, name: str) -> any:
        return self.kwargs[name]
