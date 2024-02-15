from dataclasses import dataclass
import json
from pathlib import Path


@dataclass
class SchemaHelper:
    subset: bool


class SchemaError(ValueError):
    pass


class MismatchingSchemaError(ValueError):
    pass


def json_schema(path: str, subset: bool = False):
    def decorator(class_):
        path_ = Path(path)
        upgrade = False
        create = not path_.exists() or upgrade
        main_model_schema = make_schema(class_)
        if create:
            with path_.open("w") as f:
                json.dump(main_model_schema, f, indent=2)
                f.write("\n")
        else:
            with path_.open("r") as f:
                read_model = json.load(f)
            if read_model != main_model_schema:
                raise MismatchingSchemaError("Not matching!")
        return class_

    return decorator

def make_schema(class_):
    return class_.model_json_schema()


@dataclass
class SchemaComparisonResult:


    @property
    def equal(self):
        return True

def compare_schemas(schema1, schema2):
    print('X', schema1)
    print('Y', schema2)

    s1 = set(schema1['properties'].keys())
    s2 = set(schema1['properties'].keys())

    if s1 - s2:
        print('S1 - s2', s1-s2)

    if s2 - s1:
        print('S2 - s1', s2-s1)

    return SchemaComparisonResult()
