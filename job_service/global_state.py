import json

class GlobalClass:
    def __init__(self, name):
        self.name = name
        self.shared_data = {}

    def modify_data(self, key: str, value):
        self.shared_data[key] = value

    def get_data(self, key: str):
        return self.shared_data.get(key)
    
    def to_json(self):
        """
        Serialize the object's state to a JSON string.
        Removes Logger when serializing.
        """
        x = {}
        non_serializable = ["logger"] # Add more incase main process has more complicated objects

        for key in self.shared_data:
            if (key not in non_serializable):  
                x[key] = self.shared_data[key]

        obj_dict = {
            "name": self.name,
            "shared_data": x
        }
        
        return json.dumps(obj_dict)
    
    @classmethod # The method is bound to the class and not to an instance of the class.
    def from_json(cls, json_str):
        """
        Deserialize from a JSON string to a new GlobalClass instance.
        """
        obj_dict = json.loads(json_str)
        instance = cls(obj_dict["name"])
        instance.shared_data = obj_dict["shared_data"]

        return instance

# Instantiate the global class
global_instance = GlobalClass("main")