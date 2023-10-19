# SELECT Query

Syntax: `SELECT <array of relative paths> [FROM <relative path>] [WHERE <condition>] [INSTANCEOF <schema url>]`

Used to return a set of nodes from your user node, or nodes from a set matching a particular path.

## The FROM and INSTANCEOF directives

The FROM directive is particularly useful when combined with the INSTANCEOF directive. The relative path `**` will select every node in the database, and instanceof can filter these by what schema they conform to. This allows use of the SELECT query almost identically to how it is used in traditional relational databases.

## Examples

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
