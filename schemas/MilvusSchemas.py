from typing import Any
from pymilvus import CollectionSchema, FieldSchema, DataType


class DeliriumNetworkSchema:
    index_field = "vector"
    index_params = {
        "metric_type": "IP",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 1024},
    }

    def __call__(self, *args: Any, **kwds: Any) -> CollectionSchema:
        return CollectionSchema(
            fields=[
                FieldSchema(
                    name="pk", dtype=DataType.INT64, is_primary=True, auto_id=True
                ),
                FieldSchema(name="source", dtype=DataType.JSON),
                FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
                FieldSchema(
                    name=self.index_field, dtype=DataType.FLOAT_VECTOR, dim=384
                ),
            ],
            enable_dynamic_field=True,
        )
