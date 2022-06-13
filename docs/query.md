# Query Language

DDS uses a SQL-like query language with a few general differences caused by the fact that DDS is a graph, not relational database.

+ Instead of column names, we use paths to match properties
+ There are no inner queries or joins, paths allow you to retrieve any related data you want

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

## Commands

### SELECT

Syntax: `SELECT <array of relative paths> [FROM <relative path>] [WHERE <condition>] [INSTANCEOF <schema url>]`

Used to return a set of values optionally from a set of all nodes, or nodes matching a certain path.

#### FROM

Syntax 

NOTE:

Don't use a query like `SELECT {84bd7397-d9c9-44a2-b668-cfac0329766c}` to select a particular node, as this will attempt to select a relative path on *EVERY* node in the database. The correct way to select a single node is the following: `SELECT . FROM {84bd7397-d9c9-44a2-b668-cfac0329766c}`. This selects the `.` or "Self" identifier from the set of nodes that match the path `{84bd7397-d9c9-44a2-b668-cfac0329766c}`. This will select and return a single node with ID `{84bd7397-d9c9-44a2-b668-cfac0329766c}`.

Example 0:

```
SELECT . WHERE name == "Peter Evans"
```
Selects all nodes where node's name is "Peter Evans"

Example 1:

```
SELECT email_address, name WHERE name/first_name == "Peter" INSTANCEOF "https://schemas.auxiliumsoftware.co.uk/v1/user.json"
```
Selects the email address and name for all users where their first name is "Peter"

### GRANT

Example 0:

```
GRANT WRITE, READ TO {87cdd424-295d-49ec-8720-719fa6ada18f} WHERE email_address == "test@example.com"
```

Example 1:

```
GRANT READ TO {87cdd424-295d-49ec-8720-719fa6ada18f} ON {f371fcbb-e101-48ed-aa5f-7d1a0c48faaf}
```
