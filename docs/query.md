# Query Language

deegraph uses a SQL-like query language with a few general differences caused by the fact that deegraph is a graph, not relational database.

+ Instead of column names, we use paths to match properties
+ There are no inner queries or joins, paths allow you to retrieve any related data you want

When constructing conditions, you may want to refer to the [Condition documentation](conditions.md)

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

## Commands

### SELECT

Syntax: `SELECT <array of relative paths> [FROM <relative path>] [WHERE <condition>] [INSTANCEOF <schema url>]`

Used to return a set of nodes from your user node, or nodes from a set matching a particular path.

#### The FROM and INSTANCEOF directives

The FROM directive is particularly useful when combined with the INSTANCEOF directive. The relative path `**` will select every node in the database, and instanceof can filter these by what schema they conform to. This allows use of the SELECT query almost identically to how it is used in traditional relational databases.

#### Examples

Example 0:

```
SELECT . WHERE name = "Peter Evans" FROM **
```
Selects all nodes where node's name is "Peter Evans"

Example 1:

```
SELECT email_address, name WHERE display_name = "Peter" FROM ** INSTANCEOF "https://schemas.auxiliumsoftware.co.uk/v1/user.json"
```
Selects the email address and name for all users where their first name is "Peter"

### GRANT

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
