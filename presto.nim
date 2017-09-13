import tables
import strutils
import sequtils
import httpclient
import json

proc range(arg: int): seq[int] = 
    result.newSeq(arg)

type
    ResultSet = ref object
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

method close(this: Connection) {.base.} =
    discard

method commit(this: Connection) {.base.} =
    discard

method cursor*(this: Connection): Cursor {.base.} =
    return this.cur

method fetchOne*(this: Cursor): seq[string] {.base.} =
    if this.resultSet.data.len == 0:
        raise newException(IndexError, "No Rows")
    return this.resultSet.data.pop()

method fetchAll*(this: Cursor): seq[seq[string]] {.base.} =
    return this.resultSet.data

method processResponse(this: Cursor, response: Response) {.base.} =
    if response.status != "200 OK":
        raise newException(HttpRequestError, "Status code returned bad.")
    let data = parseJson(response.body)
    if data.hasKey("error"):
        raise newException(SystemError, data["error"].str)
    var state = data["stats"]["state"].str
    if not data.hasKey("nextUri"):
        this.resultSet.nextUri = ""
        this.resultSet.state = "FINISHED"
        return
    if not data.hasKey("columns"):
        this.resultSet.nextUri = data["nextUri"].str
        this.resultSet.state = state
        this.resultSet.data = @[]
        this.resultSet.columns = @[]
        return
    if data.hasKey("data"):
        if data.hasKey("nextUri"):
            var nextUri = data["nextUri"].str
            this.resultSet.nextUri = nextUri
        else:
            this.resultSet.nextUri = ""
        var columns = data["columns"].mapIt(it["name"].str)
        var dataset = data["data"].getElems
        for data in dataset:
            this.resultSet.data.add(data.mapIt(it.getStr))
        this.resultSet.state = state
        this.resultSet.columns = columns
    if data.hasKey("nextUri") and not data.hasKey("data"):
        var nextUri = data["nextUri"].str
        this.resultSet.nextUri = nextUri
        this.resultSet.state = state
    return

method rollback(this: Connection) {.base.} =
    raise newException(ValueError, "Presto does not have transcations")

method execute*(this: Cursor, query: string) {.base.} =
    # Adding some presto session properties is seted.
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
    if this.resultSet.state == "STARTED":
        var response = client.request(this.resultSet.nextUri, httpMethod = HttpPost, body = query)
        this.processResponse(response)
    while true:
        var response = client.request(this.resultSet.nextUri, httpMethod = HttpGet)
        this.processResponse(response)
        if this.resultSet.state == "FINISHED":
            break

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
    var con = connect("host", 8889, "hive", "default", "benny")
    var cur = con.cursor()
    cur.execute("query")
    echo(cur.fetchOne())