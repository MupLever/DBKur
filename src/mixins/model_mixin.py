from pydantic import ConfigDict, BaseModel


class LowerCaseMixin(BaseModel):
    """Миксин для перевода атрибутов в нижний регистр"""

    model_config = ConfigDict(alias_generator=lambda field_name: field_name.lower())
