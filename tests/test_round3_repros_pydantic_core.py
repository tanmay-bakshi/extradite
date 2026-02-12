"""Minimal reproduction tests for remaining extradite/pydantic-core gaps."""

from extradite import extradite


def test_local_class_handle_satisfies_is_subclass_schema() -> None:
    """Local class handles should satisfy pydantic-core ``is-subclass`` schema ``cls`` requirements."""
    schema_validator_type: type = extradite("pydantic_core:SchemaValidator", transport_policy="value")
    validator: object | None = None
    try:
        class BaseLocal:
            """Local base class used to force parent-handle transport."""

        class SubLocal(BaseLocal):
            """Local subclass used as ``is-subclass`` validation input."""

        validator = schema_validator_type({"type": "is-subclass", "cls": BaseLocal})
        result: object = validator.validate_python(SubLocal)
        assert result is SubLocal
    finally:
        if validator is not None:
            validator.close()
        schema_validator_type.close()


def test_local_class_handle_satisfies_model_schema_cls() -> None:
    """Local class handles should satisfy pydantic-core ``model`` schema ``cls`` requirements."""
    schema_validator_type: type = extradite("pydantic_core:SchemaValidator", transport_policy="value")
    validator: object | None = None
    try:
        class LocalModel:
            """Local model class used to force parent-handle transport."""

            def __init__(self, **kwargs: object) -> None:
                """Initialize model instance.

                :param kwargs: Field values.
                """
                for key, value in kwargs.items():
                    setattr(self, key, value)

        schema: dict[str, object] = {
            "type": "model",
            "cls": LocalModel,
            "schema": {
                "type": "model-fields",
                "fields": {
                    "x": {
                        "type": "model-field",
                        "schema": {"type": "int"},
                    }
                },
                "model_name": "LocalModel",
                "computed_fields": [],
            },
            "custom_init": False,
            "root_model": False,
            "config": {},
        }

        validator = schema_validator_type(schema)
        instance: object = validator.validate_python({"x": "1"})
        assert instance.__class__ is LocalModel
        assert getattr(instance, "x") == 1
    finally:
        if validator is not None:
            validator.close()
        schema_validator_type.close()

