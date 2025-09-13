from pydantic import BaseModel, Field, ValidationError
from typing import List,Any,Dict
import json


class DatasetInfo(BaseModel):
    dataset_name:str
    dataset_type:str
    linked_service_name:str
    linked_service_type:str

class ActivityInfo(BaseModel):
    name:str
    datasets:List[DatasetInfo]

class PipelineInfo(BaseModel):
    name:str
    activities:List[ActivityInfo]

    @classmethod
    def from_json_file(cls, filepath: str) -> 'PipelineInfo':
        """Load one or more pipelines from a JSON file."""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return [cls.model_validate(item) for item in data]
        else:
            return cls.model_validate(data)

