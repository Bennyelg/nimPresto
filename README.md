# prestodb-Nim 

Simple presto-db connector using nim.

## Usage:

```nim
    var con = connect("host", 8889, "hive", "default", "benny")
    var cur = con.cursor()
    cur.execute("SELECT NOW()")
    echo(cur.fetchOne())
```