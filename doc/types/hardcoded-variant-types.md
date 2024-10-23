# Hardcoded variant types

## Uber variant type

To provides "dynamic" data per Uniq, blockchain introduces a variant type that contains almost all existing types (primitive and arrays).

There is two constraints on the avro side here:
* Adding a new type to an existing avro union is a breaking change; so we must provide an implementation that should never (never says never...) change.
* We cannot manage logicalType since we can only have one instant of a type (int, double, etc.) in the Union.

To make it easier we decided to hardcode the uber variant type into an avro schema that contains all possible primitive type and array.

The only downside is that we rely on looking up the variant name within the type. Therefore, if the type changes, the code will need to be updated accordingly.