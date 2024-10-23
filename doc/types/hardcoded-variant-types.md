# Hardcoded variant types

## Uber variant type

To provide "dynamic" data per Uniq, blockchain introduces a variant type that includes almost all existing types (primitives and arrays).

There are two constraints on the Avro side:

* Adding a new type to an existing Avro union is a breaking change, so we must implement a solution that ideally will never need to change (although never say never...).
* We cannot manage logical types because only one instance of a type (int, double, etc.) can exist in the union.

To simplify things, we decided to hardcode the "uber" variant type into an Avro schema that contains all possible primitive types and arrays.

The only downside is that this approach relies on looking up the variant name within the type. Therefore, if the type changes, the code will need to be updated accordingly.