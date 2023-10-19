# GRANT Query

Syntax: `GRANT <permission type>[, <permission type>]... ON <relative path> WHERE <condition> [DELEGATABLE]`

Used to grant permissions on the database.

## The DELEGATABLE directive

Only DELEGATABLE permission grants can be given to ther users by non-root users.

## Permission types

### WRITE

Allows a node to modify properties of a node, and to add or remove links.

### READ

Allows a node the ability to read a resource.

### DELETE

Allows destruction of nodes.

### ACT

Allows a node to act as though it was another node (assuming the target node's permissions, wether giving more or less permissions).

## Examples

Example 0:

```
GRANT WRITE, READ TO {87cdd424-295d-49ec-8720-719fa6ada18f} WHERE email_address == "data:text/plain,test%40example.com"
```
Grants read and write permissions where the subject email_address exactly matches the data url for "test@example.com"

Example 1:

```
GRANT READ TO {87cdd424-295d-49ec-8720-719fa6ada18f} ON {f371fcbb-e101-48ed-aa5f-7d1a0c48faaf}
```
Grants read permissions on one node to another
