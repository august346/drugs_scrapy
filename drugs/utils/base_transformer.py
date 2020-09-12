class Transformer:
    def __init__(self, item):
        self._item = item

    def get_transformed_item(self):
        raise NotImplementedError
