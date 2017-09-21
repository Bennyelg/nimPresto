
# prestodb-Nim [![nimble](https://raw.githubusercontent.com/yglukhov/nimble-tag/master/nimble.png)](https://github.com/yglukhov/nimble-tag)

![alt tag](https://github.com/Bennyelg/nimPresto/blob/master/presto_nim.jpg)

Simple presto-db connector using nim.


## What Works ?
* open
* execute(sql"sql")
* fetchOne(asTable=true/false)
* fetchMany(size, asTable=true/false)
* fetchAll(asTable=true/false)
* getColumns()

## Usage:

```nim
    import db_presto
    let conn = open(host="HOST", port=port, catalog="hive", schema="dwh", username="benny")
    defer: con.close()
    var cur = con.cursor()
    cur.execute(sql"SELECT NOW()")
    echo(cur.fetchOne())
```

## Installation:

```bash
nimble install db_presto
```

## contributing 

I'll be happy to get any help, just work & pull request.
