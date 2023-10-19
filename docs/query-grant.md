# GRANT Query

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
