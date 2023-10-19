# Query Language

deegraph uses a SQL-like query language with a few general differences caused by the fact that deegraph is a graph, not relational database.

+ Instead of column names, we use paths to match properties
+ There are no inner queries or joins, paths allow you to retrieve any related data you want

When constructing conditions, you may want to refer to the [Condition documentation](conditions.md)

## Commands

+ [SELECT](query-select.md)
+ [DIRECTORY](query-directory.md)
+ [REFERENCES](query-references.md)

+ [LINK](query-link.md)
+ [UNLINK](query-unlink.md)

+ [PUT](query-put.md)
+ [INSERT](query-insert.md)
+ [DELETE](query-delete.md)

+ [GRANT](query-grant.md)
+ [PERMISSIONS](query-permissions.md)

## Paths

Node paths are written similarly to UNIX directory paths. The "current directory" is contextual as you would expect, and is used when matching against many nodes. The "root directory" is mapped to your own user node.

Example 0:

```
/name
```
Returns your own name.

Example 1:

```
subject/@id
```
Returns the ID of the subject of a case you're currently running a SELECT query on for example.

Example 2:
```
{84bd7397-d9c9-44a2-b668-cfac0329766c}/name
```
Returns the name of the node with ID `{84bd7397-d9c9-44a2-b668-cfac0329766c}`.

Example 3:
```
{84bd7397-d9c9-44a2-b668-cfac0329766c}/messages/#
```
Returns the set of messages that the user `{84bd7397-d9c9-44a2-b668-cfac0329766c}` has access to as an array.

Example 4:

```
{84bd7397-d9c9-44a2-b668-cfac0329766c}/@creator/*
```
Returns all info about the creator of the node {84bd7397-d9c9-44a2-b668-cfac0329766c}.
