
# prestodb-Nim [![nimble](https://raw.githubusercontent.com/yglukhov/nimble-tag/master/nimble.png)](https://github.com/yglukhov/nimble-tag)

![alt tag](https://github.com/Bennyelg/nimPresto/blob/master/presto_nim.jpg)

Simple presto-db connector using nim.

## Current Version:

Release 1.0

## What Works ?
* execute(sql"sql")
* fetchOne()
* fetchMany(size)
* fetchAll()
* authentication

## Usage:

```nim
    import db_presto
    let conn = newPrestoClient("https", "host", "8443", "hive", "default", "user", "password")
    var ctx = conn.cursor()
    ctx.execute(sql"select * from gett_algo.weather_forecast5d3h_v2 LIMIT 11")
```

## Installation:

```bash
nimble install db_presto
```

## contributing

I'll be happy to get any help, just work & pull request.
