# Conditions

Conditions in Deegraph can always be combined using logical operators. Type coercion is a feature of all numerical operators such as greater than, and specifically in the "equals" operator.

## Equality operators

"=", "==" and "===" all have significantly different meanings in Deegraph. Despite this they all share one property: when comparing sets (i.e. when a path contains a glob such as "\*") it will return true if *any* of the pairs across the sets satisfy the equality condition.

"=" or "EQUALS" is the most commonly used equality operator. It performs [type coercion](type-coercion.md) automatically on both sides. It compares values as expected by the user.
It automatically parses internal data urls for you, but cannot be used to compare abritrary urls. Arbitrary urls will resolve to an empty string.

"==" or "IDENTICAL" is useful when you might encounter arbitrary urls. It does not perform type coercion, and will match the exact value of the node. This means the data urls will be compared directly. An important caveat is that data urls that *resolve* to the same data, may not match here. For example "+" and "%20" both encode a space character. With the "EQUALS" operator, these will match, but with the "IDENTICAL" operator, they will not.

"===" or "IS" is only usable for comparing nodes, not literal values. It only matches when the paths on either side resolve to the *same exact node* not just when their *contents* match.

## Inequality operators

"!=" or "DIFFERENT" is equivalent to a logical not applied to an "EQUALS" operator

"!==" is equivalent to a logical not applied to an "IDENTICAL" operator

"ISNT" is equivalent to a logical not applied to an "IS" operator

## Numerical comparitors

">", "<", ">=" and "<=" all work as expected, and will force both sides to be [coerced to the number type](type-coercion.md).

## Logical operators

### NOT operator

"!" or "NOT" can be prepended to any statement to evaluate it as a boolean and invert the result.

### AND operator

"&&" or "AND" can be used as an infix operator to perform a logical and operation on boolean casted values.

### OR operator

"||" or "OR" can be used as an infix operator to perform a logical or operation on boolean casted values.

### XOR operator

"^|" or "XOR" can be used as an infix operator to perform a logical xor operation on boolean casted values.
