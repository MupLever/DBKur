from pydantic import ConfigDict, BaseModel


class BaseModelMixin(BaseModel):
    model_config = ConfigDict(alias_generator=lambda field_name: field_name.lower())
