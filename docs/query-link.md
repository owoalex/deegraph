# LINK Query

Syntax: `LINK <relative path> TO/OF <relative path> AS <property name> [OVERWRITE/REPLACE/FORCE]`

Links existing nodes as properties of other nodes.

## The OVERWRITE/REPLACE/FORCE directive

By default, the LINK command will skip linking any node where there is already a property present. The REPLACE directive overrides this behaviour, forcing replacement of any property that already exists.
