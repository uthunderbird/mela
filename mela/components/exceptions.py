class NackMessageError(Exception):

    def __init__(self, message: str, requeue: bool = True):
        super().__init__(message, f"requeue: {requeue}")
        self.requeue: bool = requeue
