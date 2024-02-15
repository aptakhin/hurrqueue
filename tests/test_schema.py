import pytest
from pydantic import BaseModel

from snailqueue.schema import MismatchingSchemaError, json_schema, make_schema, compare_schemas


def test_schema_interface_simple_valid():
    @json_schema("my_test_class.json")
    class MyClass(BaseModel):
        p: int

    @json_schema("my_test_class.json")
    class MyClass(BaseModel):
        p: int


def test_schema_interface_simple_mismatching():
    @json_schema("my_test_class.json")
    class MyClass(BaseModel):
        p: int

    with pytest.raises(MismatchingSchemaError):
        @json_schema("my_test_class.json")
        class MyClass(BaseModel):
            p: str


def test_compare_cases():
    class MyClass(BaseModel):
        p: int
    schema_1 = make_schema(MyClass)

    class MyClass(BaseModel):
        p: int
    schema_2 = make_schema(MyClass)

    compare_result = compare_schemas(schema_1, schema_2)
    assert compare_result.equal



def test_compare_cases_2():
    class MyClass(BaseModel):
        p: int
    schema_1 = make_schema(MyClass)

    class MyClass(BaseModel):
        p: int
        m: int
    schema_2 = make_schema(MyClass)

    compare_result = compare_schemas(schema_1, schema_2)
    assert not compare_result.equal
