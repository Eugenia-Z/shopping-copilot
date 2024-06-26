from base64 import b64encode
import json
class Extraction:
    def __init__(self):
        self.encoding = "utf-8"
        
    def DataParse(json_input):
        data = json.loads(json_input)
        name = data['name']['value']
        types = "=".join([f"{type_['name']}" for type_ in data['types']])
        dominant_label = "=".join([label['text'] for label in data['dominant_label']])
        facts_dict = {fact['name']: "+".join([value['text'] for value in fact['values']]) for fact in data['facts']}
        facts_str= "=".join([f"{key}_{value}" for key, value in facts_dict.items()])
        
        return name, types, dominant_label, facts_str
    
    def Process(self,inputRow, outputRow):
        Satorid = inputRow["Satorid"]
        Data = inputRow["Data"]
        Name, Types, Dominant_label, Facts = self.DataParse(Data)
        
        outputRow["Satorid"] = Satorid
        outputRow["Name"] = Name
        outputRow["Types"] = Types
        outputRow["Dominant_label"] = Dominant_label
        outputRow["Facts"] = Facts
        
        return outputRow