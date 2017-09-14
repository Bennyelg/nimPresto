import tables
import strutils
import sequtils
import httpclient
import json
import os

#[
    PrestoDb Connector
    Writer: elgazarbenny at gmail.com
    description: Simple Naive implementation in-order to use presto-db database.
]#

proc range(size: int): seq[int] = 
    result.newSeq(size)

type NoConnectionError = object of Exception
type CursorError = object of Exception
type NoTransactionError = object of Exception

type
    ResultSet = ref object
        client: HttpClient
        nextUri: string
        state: string
        columns: seq[string]
        data: seq[seq[string]]
    
type
    Cursor = ref object
        catalog: string
        schema: string
        source: string
        sessionProps: string
        poolInterval: string
        username: string
        host: string
        port: string
        resultSet: ResultSet

type
    Connection = ref object of RootObj
        host: string
        port: int
        cur: Cursor

proc close*(this: Connection)  =
    # There is no Actual close so just cleaning the connection information. 
    this.cur = Cursor()
    this.host = ""
    this.port = -1
    discard

proc commit*(this: Connection)  =
    discard

proc cursor*(this: Connection): Cursor  =
    return this.cur

proc getColumns*(this: Cursor): seq[string] =
    return this.resultSet.columns

proc processResponse(this: Cursor, response: Response)  =
    if response.status != "200 OK":
        raise newException(NoConnectionError, "Status code returned bad. %s" % response.status)
    let data = parseJson(response.body)
    if data.hasKey("error"):
        raise newException(CursorError, data["error"].getStr)
    var state = data["stats"]["state"].str
    if not data.hasKey("nextUri"):
        this.resultSet.nextUri = ""
        this.resultSet.state = "FINISHED"
        return
    if not data.hasKey("columns"):
        this.resultSet.nextUri = data["nextUri"].getStr
        this.resultSet.state = state
        this.resultSet.data = @[]
        this.resultSet.columns = @[]
        return
    if data.hasKey("data"):
        if data.hasKey("nextUri"):
            var nextUri = data["nextUri"].getStr
            this.resultSet.nextUri = nextUri
        else:
            this.resultSet.nextUri = ""
        var columns = data["columns"].mapIt(it["name"].str)
        var dataset = data["data"].getElems
        this.resultSet.data = @[]
        for data in dataset:
            this.resultSet.data.add(data.mapIt(it.getStr))
        this.resultSet.state = state
        this.resultSet.columns = columns
    if data.hasKey("nextUri") and not data.hasKey("data"):
        var nextUri = data["nextUri"].getStr
        this.resultSet.nextUri = nextUri
        this.resultSet.state = state
    return

proc rollback*(this: Connection)  =
    raise newException(NoTransactionError, "Presto does not have transcations")

proc execute*(this: Cursor, query: string)  =
    this.resultSet = ResultSet(nextUri: "", state: "STARTED", columns: @[])
    var additional: seq[string] = @[]
    if this.sessionProps.len != 0:
        var additionalProperties = this.sessionProps.split(",")
        var k = range(additionalProperties.len)
        var evens = filter(zip(k, additionalProperties), proc (x: (int, string)): bool = x[0] mod 2 == 0)
        var odds = filter(zip(k, additionalProperties), proc (x: (int, string)): bool = x[0] mod 2 != 0)
        for ind in countup(0, evens.len - 1):
            additional.add(format("$1=$2", evens[ind][1], odds[ind][1]))   
    var client = newHttpClient()
    var protocol = "http://"
    var url = protocol & this.host & format(":$1", this.port) & "/v1/statement/"
    client.headers = newHttpHeaders()
    client.headers.add("X-Presto-Catalog", this.catalog)
    client.headers.add("X-Presto-Schema", this.schema)
    client.headers.add("X-Presto-Source", this.source)
    client.headers.add("X-Presto-User", this.username)
    if additional.len > 0:
        client.headers.add("X-Presto-Session", additional.join(","))
    this.resultSet.nextUri = url
    this.resultSet.client = client
    var response = client.request(this.resultSet.nextUri, httpMethod = HttpPost, body = query)
    this.processResponse(response)

proc fetchOne*(this: Cursor): seq[string] =
    if this.resultSet.state == "FINISHED":
        raise newException(CursorError, "No More Rows.")
    if this.resultSet.data.len > 0:
        return this.resultSet.data.pop()
    elif this.resultSet.data.len == 0:
        os.sleep(500)  # Sleeps 500 miliseconds before each request.
        var response = this.resultSet.client.request(this.resultSet.nextUri, httpMethod = HttpGet)
        this.processResponse(response)

    return this.resultSet.data.pop()

proc fetchMany*(this: Cursor, amount: int): seq[seq[string]]  =
    var setOfResults: seq[seq[string]] = @[]
    for _ in countup(1, amount):
        var element = this.fetchOne()
        setOfResults.add(element)
    return setOfResults

proc fetchAll*(this: Cursor): seq[seq[string]] =
    var queryData: seq[seq[string]] = @[]
    while true:
        if this.resultSet.state == "FINISHED":
            break
        if this.resultSet.data.len > 0:
            queryData.add(this.resultSet.data)
        # Sleeps 500 miliseconds before each request.
        os.sleep(100)
        var response = this.resultSet.client.request(this.resultSet.nextUri, httpMethod = HttpGet)
        this.processResponse(response)
    return queryData

proc connect*(host: string, port: int, catalog: string, schema: string,
              username: string, source = "NimPresto", sessionProps = ""): Connection =
    let cursor = Cursor(catalog: catalog,
                        schema: schema,
                        source: source, 
                        sessionProps: sessionProps,
                        poolInterval: "1", 
                        username: username,
                        host: host,
                        port: port.intToStr)
    return Connection(
        host: host,
        port: port,
        cur: cursor
    )


when isMainModule:
    let con = connect("HOST", 8889, "hive", "dwh", "benny")
    defer: con.close()
    var cur = con.cursor()
    cur.execute("SQL")
    #echo(cur.getColumns())
    # for s in cur.fetchMany(10):
    #     echo(s)
    # echo(cur.fetchOne())
    # echo("===")
    # echo(cur.fetchOne())