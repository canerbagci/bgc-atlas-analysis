# bgc-atlas-analysis

[![DOI](https://zenodo.org/badge/842929533.svg)](https://doi.org/10.5281/zenodo.13903803)

## Database Configuration

`Database.java` reads the connection URL, user and password from environment
variables. Set the following variables before running the application:

```
export DB_URL="jdbc:postgresql://localhost:5432/atlas"
export DB_USER="myuser"
export DB_PASSWORD="secret"
```

If these variables are not present, the class looks for a `db.properties`
file in the working directory with the keys `db.url`, `db.user` and
`db.password`.
