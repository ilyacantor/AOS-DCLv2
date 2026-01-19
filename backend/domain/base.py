"""
Base Pydantic models with camelCase serialization support.
"""

from pydantic import BaseModel, ConfigDict
from humps import camelize


def to_camel(string: str) -> str:
    """Convert snake_case to camelCase."""
    return camelize(string)


class CamelCaseModel(BaseModel):
    """
    Base model that serializes to camelCase for API responses.

    Usage:
        class MyModel(CamelCaseModel):
            my_field: str  # Serializes as "myField" in JSON
    """
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,  # Allow both snake_case and camelCase on input
        from_attributes=True,
    )
