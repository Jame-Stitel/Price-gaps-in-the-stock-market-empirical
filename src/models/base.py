from pydantic import BaseModel


def to_camel(string: str) -> str:
    words = string.split('_')
    camelized = ''.join(word.capitalize() for word in words)
    return camelized


class CamelAliasModel(BaseModel):
    class Config:
        alias_generator = to_camel
        allow_population_by_field_name = True
