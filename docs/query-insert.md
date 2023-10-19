# INSERT Query

Syntax: `INSERT INTO <relative path> [KEYS <key>[, <key>]...] [SCHEMAS <schema>[, <schema>]...] [VALUES <uri>[, <uri>]...] [DUPLICATE] [REPLACE]`

Used to insert a lot of data (think whole tables) at once.

## The DUPLICATE directive

The DUPLICATE directive will make seperate nodes for every match on the "INTO" path. This is particularly important when inserting into paths with globs. Without this directive, the same node will be linked to every node that matches the "INTO" directive.

## The REPLACE directive

By default, the INSERT command will skip inserting any node where there is already a property present. The REPLACE directive overrides this behaviour, forcing replacement of any property that already exists.
