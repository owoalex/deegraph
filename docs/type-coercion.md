# Type Coercion

## Automatic coercion logic

Automatic coercion used in the "=" operator is based on a priority system. This starts by identifying data types on either side using the following rules:
- The strings "TRUE" and "FALSE" regardless of case will be interpreted as bool.
- ISO dates will be interpreted as number.
- Decimal respresentation will be interpreted as a number.
- Hexadecimal representation of an integer prefixed with "0x" will be interpreted as a number.
- All other data will be interpreted as string.

Once this identification is done, the following steps are taken in order:
- If either side is of type bool, compare by attempting to cast both sides to bool.
- If either side is of type number, compare by attempting to cast both sides to number.
- Finally, compare both sides with a plain string comparison if no other rule has taken priority.

## Coercion to numerical type

- ISO dates are transformed into UNIX timestamp representation. Dates will be converted to the number of seconds since the 1st of January 1970. Fractional seconds are resolved correctly.
- Decimal numbers with or without a fractional component will be resolved to doubles. NOTE: Trailing decimal points are not allowed and will result in a parse error.
- Hexadecimal numbers prefixed with "0x" will resolve to doubles. Fractional representations are not allowed.
- The strings "TRUE" and "FALSE" regardless of case resolve to 1 and 0 respectively.
- Attempting to forcibly coerce an incompatible value will result in a parse error.

## Coercion to boolean type

- The strings "TRUE" and "FALSE" regardless of case resolve as expected.
- Anything that may resolve to a numerical type will be casted to TRUE to values above 0.5, other values will return FALSE.
- Attempting to forcibly coerce an incompatible value will result in a parse error.
