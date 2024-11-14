class GlobalClass:
    def __init__(self):
        self.shared_data = {}

    def modify_data(self, key: str, value):
        self.shared_data[key] = value

    def get_data(self, key: str):
        return self.shared_data.get(key)

# Instantiate the global class
global_instance = GlobalClass()