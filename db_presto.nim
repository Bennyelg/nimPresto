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
type QueryExecutionError = object of Exception
type NoTransactionError = object of Exception
type NotValidProtocolError = object of Exception

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
        poolInterval: int
        username: string
        tableCursor: bool
        host: string
        protocol: string
        port: string
        resultSet: ResultSet

type
    Connection = ref object of RootObj
        host: string
        port: int
        timeOutInSeconds: int
        cur: Cursor

proc ping(this: Connection): bool =
    var client = newHttpClient(timeout=this.timeOutInSeconds * 100)
    var url = this.cur.protocol & "://" & this.host & ":" & this.port.intToStr
    if client.get(url).status != "200 OK":
        return false
    return true

proc close*(this: Connection)  =
    # There is no Actual close so just cleaning the connection information. 
    this.cur = Cursor()
    this.host = ""
    this.port = -1
    discard

proc commit*(this: Connection)  =
    raise newException(NoTransactionError, "Presto does not have transcations.")

proc cursor*(this: Connection): Cursor  =
    return this.cur

proc rollback*(this: Connection)  =
    raise newException(NoTransactionError, "Presto does not have transcations.")

proc getColumns*(this: Cursor): seq[string] =
    return this.resultSet.columns

proc processResponse(this: Cursor, response: Response)  =
    if response.status != "200 OK":
        raise newException(NoConnectionError, "Status code returned bad. %s" % response.status)
    let data = parseJson(response.body)
    if data.hasKey("error"):
        raise newException(QueryExecutionError, data["error"]["message"].getStr)
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

proc execute*(this: Cursor, query: string)  =
    var additional: seq[string] = @[]
    if this.sessionProps.len != 0:
        var additionalProperties = this.sessionProps.split(",")
        var k = range(additionalProperties.len)
        var evens = filter(zip(k, additionalProperties), proc (x: (int, string)): bool = x[0] mod 2 == 0)
        var odds = filter(zip(k, additionalProperties), proc (x: (int, string)): bool = x[0] mod 2 != 0)
        for ind in countup(0, evens.len - 1):
            additional.add(format("$1=$2", evens[ind][1], odds[ind][1]))
    var client = newHttpClient()
    var url = this.protocol & "://" & this.host & format(":$1", this.port) & "/v1/statement/"
    client.headers = newHttpHeaders()
    client.headers.add("X-Presto-Catalog", this.catalog)
    client.headers.add("X-Presto-Schema", this.schema)
    client.headers.add("X-Presto-Source", this.source)
    client.headers.add("X-Presto-User", this.username)
    if additional.len > 0:
        client.headers.add("X-Presto-Session", additional.join(","))
    this.resultSet = ResultSet(nextUri: url, state: "STARTED", columns: @[], client: client)
    var response = client.request(this.resultSet.nextUri, httpMethod = HttpPost, body = query)
    this.processResponse(response)

proc fetchOne*(this: Cursor): seq[string] =
    if this.resultSet.state == "FINISHED":
        raise newException(CursorError, "No More Rows.")
    
    if this.resultSet.data.len > 0:
        return this.resultSet.data.pop()
    elif this.resultSet.data.len == 0:
        os.sleep(this.poolInterval)
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
        os.sleep(this.poolInterval)
        var response = this.resultSet.client.request(this.resultSet.nextUri, httpMethod = HttpGet)
        this.processResponse(response)
    return queryData

proc open*(host: string, port: int, protocol: string = "http", catalog: string, schema: string,
           username: string, source = "NimPresto", poolInterval = 1,
           sessionProps = "", tableCursor = false): Connection =
    if protocol notin ["http", "https"]:
        raise newException(NotValidProtocolError, "The protocol you specified is not valid protocol: %s" % protocol)
    let cursor = Cursor(catalog: catalog,
                        schema: schema,
                        source: source, 
                        sessionProps: sessionProps,
                        poolInterval: poolInterval * 1000,
                        tableCursor: tableCursor,
                        username: username,
                        host: host,
                        protocol: protocol,
                        port: port.intToStr)
    let connection = Connection(
        host: host,
        port: port,
        timeOutInSeconds: 10,
        cur: cursor
    )
    if not connection.ping():
        raise newException(NoConnectionError, "Failed to Established Connection.")
    return connection


when isMainModule:
    let con = open(host="host", port=8889, catalog="hive", schema="dwh", username="benny")
    defer: con.close()
    var cur = con.cursor()
    cur.execute("SELECT * FROM abc")
    echo(cur.fetchOne())
    #echo(cur.getColumns())
    # for s in cur.fetchMany(10):
    #     echo(s)
    # echo(cur.fetchOne())
    # echo("===")
    # echo(cur.fetchOne())