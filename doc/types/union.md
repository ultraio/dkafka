In avro we cannot have an union that contains the same type multiple times
like
```json
{
    "eos.type": "int8",
    "type": "int"
},
{
    "eos.type": "int16",
    "type": "int"
}
```

We need to transform it into
```json
{
    "eos.type": ["int8", "int16]",
    "type": "int"
}
```

- [ ] TODO: manage array
```json
{
    "type": "array",
    "items": {
        "eos.type": "uint64",
        "logicalType": "eos.uint64",
        "type": "long"
    }
},
{
    "type": "array",
    "items": {
        "eos.type": "float32",
        "type": "float"
    }
}
```

I can manage raw type and remove all information regarding logical type, etc.
Could it be enough if someone want to use a uin64 at the factory level? If i encode everything using an long.