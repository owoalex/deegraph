# PUT Query

Syntax: `PUT [URI/URL <uri>] [SCHEMA <schema url>] [AT <relative path>] [INTO <relative path> AS <property name>] [SAFE]`

Used to create a new node with specific properties. Either AT or INTO and AS must be set for this command to work. Setting one invalidates the other and vice versa.

## The SAFE directive

The SAFE directive will first check if the property/path is empty before creating a node. This is particularly useful when you don't want to overwrite data that may already exist.

## Examples

Example 0:

```
PUT SCHEMA "https://schemas.auxiliumsoftware.co.uk/v1/collection.json" AT {970334ed-1f4f-465a-94d7-923a99698786}/todos SAFE
```
Makes a new collection node as the property "todos", but only if it doesn't exist already.

Example 1:

```
PUT URI "data:text/plain;base64,eWlwcGVlISEh" AT {970334ed-1f4f-465a-94d7-923a99698786}/example
```
Creates a new node with the text content "yippee!!!" as the property "example".
