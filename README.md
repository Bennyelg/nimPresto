
# prestodb-Nim [![nimble](https://raw.githubusercontent.com/yglukhov/nimble-tag/master/nimble.png)](https://github.com/yglukhov/nimble-tag)

[![presto_nim](https://github.com/bennyelg/nimPresto/tree/master/presto_nim.jpg)](https://github.com/benny/nimPresto)

Simple presto-db connector using nim. (Still under heavy development.)

## What Works ?
* connect
* execute
* fetchOne
* fetchMany(size)
* fetchAll
* getColumns

## Usage:

```nim
    import presto
    let con = connect("host", 8889, "hive", "default", "benny")
    defer: con.close()
    var cur = con.cursor()
    cur.execute("SELECT NOW()")
    echo(cur.fetchOne())
```

## Installation:

```bash
nimble install presto
```


## contributing 

I'll be happy to get any help, just work & pull request.


## TODO:
* A lot of tests.
