{.experimental.}
import tables
export tables
import strutils
import sequtils
import httpclient
import json
import os
import db_common

#[
    PrestoDb Connector
    Writer: elgazarbenny at gmail.com
    description: Simple Naive implementation in-order to use presto-db database.
]#

type 
    NoConnectionError = object of Exception
    CursorError = object of Exception
    QueryExecutionError = object of Exception
    NotValidProtocolError = object of Exception
    
    SqlQuery = distinct string

    ResultSet = ref object
        client: HttpClient
        nextUri: string
        state: string
        columns: seq[string]
        data: seq[seq[string]]
  
    Cursor = ref object
        catalog: string
        schema: string
        source: string
        sessionProps: string
        pollInterval: int
        username: string
        host: string
        protocol: string
        port: string
        resultSet: ResultSet
    
    Connection = ref object of RootObj
        host: string
        port: int
        timeout: int
        cur: Cursor


using
    cur: Cursor
    con: Connection

proc ping(con): bool =
    let client = newHttpClient(timeout = con.timeout * 100)
    let url = "$1://$2:$3" % [con.cur.protocol, con.host, $con.port]
    result = client.get(url).status == Http200

proc close*(con) =
    # There is no actual close so just cleaning the connection information. 
    con.cur = Cursor()
    con.host = ""
    con.port = -1


#proc commit*(con) {.error: "Presto doesn't have transaction support".}

#proc rollback*(con) {.error: "Presto doesn't have transaction support".}

proc dbQuote(s: string): string =
  ## DB quotes the string.
  result = "'"
  for c in items(s):
    if c == '\'': add(result, "''")
    if c == '\\': add(result, "\\\\")
    else: add(result, c)
  add(result, '\'')

proc dbFormat(formatstr: SqlQuery, args: varargs[string]): string =
  result = ""
  var a = 0
  for c in items(string(formatstr)):
    if c == '?':
      if args[a] == nil:
        add(result, "NULL")
      else:
        add(result, dbQuote(args[a]))
      inc(a)
    else:
      add(result, c)


template sql*(query: string): SqlQuery = SqlQuery(query)

template cursor*(con): Cursor =
    con.cur

template getColumns*(cur): seq[string] = cur.resultSet.columns

proc processResponse(cur; response: Response) =
    if response.status != Http200:
        raise newException(NoConnectionError, "Bad response status code: $1" % response.status)
    
    let data = parseJson(response.body)
    
    if data.hasKey("error"):
        raise newException(QueryExecutionError, data["error"]["message"].str)
    
    let state = data["stats"]["state"].str
    if not data.hasKey("nextUri"):
        cur.resultSet.nextUri = ""
        cur.resultSet.state = "FINISHED"
        return
    
    if not data.hasKey("columns"):
        cur.resultSet.nextUri = data["nextUri"].getStr
        cur.resultSet.state = state
        cur.resultSet.data = @[]
        cur.resultSet.columns = @[]
        return
    
    if data.hasKey("data"):
        cur.resultSet.nextUri = data["nextUri"].getStr
        let columns = data["columns"].mapIt(it["name"].str)
        let dataset = data["data"].getElems
        cur.resultSet.data = newSeq[seq[string]](dataset.len)
        for i in 0..dataset.len - 1:
            cur.resultSet.data[i] = dataset[i].mapIt(it.getStr)
        cur.resultSet.state = state
        cur.resultSet.columns = columns
    
    if data.hasKey("nextUri") and not data.hasKey("data"):
        cur.resultSet.nextUri = data["nextUri"].getStr
        cur.resultSet.state = state

proc execute*(cur; query: SqlQuery) =
    var additional: seq[string] = @[]
    if cur.sessionProps.len != 0:
        let additionalProperties = cur.sessionProps.split(",")
        let k = newSeq[int](additionalProperties.len)
        let evens = filterIt(zip(k, additionalProperties), it[0] mod 2 == 0)
        let odds = filterIt(zip(k, additionalProperties), it[0] mod 2 != 0)
        for ind in 0..<evens.len:
            additional.add(format("$1=$2", evens[ind][1], odds[ind][1]))
      
    let client = newHttpClient()
    let url = "$1://$2:$3/v1/statement" % [cur.protocol, cur.host, cur.port]
    client.headers = newHttpHeaders(
      {
          "X-Presto-Catalog": cur.catalog,
          "X-Presto-Schema": cur.schema,
          "X-Presto-Source": cur.source,
          "X-Presto-User": cur.username
      }
    )
    if additional.len > 0:
        client.headers.add("X-Presto-Session", additional.join(","))
    cur.resultSet = ResultSet(nextUri: url, state: "STARTED", columns: @[], client: client)
    let response = client.request(cur.resultSet.nextUri, httpMethod = HttpPost, body = dbFormat(query))
    cur.processResponse(response)

proc fetchSeqOne(cur): seq[string] =
    try:
        result = cur.resultSet.data.pop()
    except IndexError:
        raise newException(CursorError, "No more rows left.")

proc fetchTableOne(cur): Table[string, string] =
    try:
        result = zip(cur.getColumns, cur.resultSet.data.pop()).toTable
    except IndexError:
        raise newException(CursorError, "No more rows left.")  

proc fetchOne*(cur; asTable: static[bool]): seq[string] | Table[string, string] =
    if cur.resultSet.state == "FINISHED":
        raise newException(CursorError, "No more rows left.")
    elif cur.resultSet.data.len == 0:
        os.sleep(cur.pollInterval)
        var response = cur.resultSet.client.request(cur.resultSet.nextUri, httpMethod = HttpGet)
        cur.processResponse(response)    
    when asTable == true:
        cur.fetchTableOne()
    else:
        cur.fetchSeqOne()

proc fetchMany*(cur; amount: int, asTable: static[bool]): seq[seq[string]] | seq[Table[string, string]] =
    when asTable == true:
        var dataSet: seq[Table[string, string]] = @[]
    else:
        var dataSet: seq[seq[string]] = @[]
    for i in 0..<amount:
        try:
            var row = cur.fetchOne(asTable)
            dataSet.add(row)
        except CursorError:
            break
    return dataSet
     

proc fetchAll*(cur; asTable: static[bool]): seq[seq[string]] | seq[Table[string, string]] =
    when asTable == true:
        var allSet: seq[Table[string, string]] = @[]
    else:
        var allSet: seq[seq[string]] = @[]

    while cur.resultSet.state != "FINISHED":
        if cur.resultSet.data.len > 0:
            for row in cur.fetchMany(cur.resultSet.data.len, asTable):
                allSet.add(row)
        os.sleep(cur.pollInterval)
        let response = cur.resultSet.client.request(cur.resultSet.nextUri, httpMethod = HttpGet)
        cur.processResponse(response)
    
    return allSet

proc open*(host: string, port: int, protocol = "http",
           catalog, schema, username: string, source = "NimPresto",
           pollInterval = 1, sessionProps = ""): Connection =
    
    if protocol notin ["http", "https"]:
        raise newException(NotValidProtocolError, "Not valid protocol: $1" % protocol)
    
    let cursor = Cursor(
      catalog: catalog,
      schema: schema,
      source: source, 
      sessionProps: sessionProps,
      pollInterval: pollInterval * 1000,
      username: username,
      host: host,
      protocol: protocol,
      port: $port
    )

    result = Connection(
        host: host,
        port: port,
        timeout: 5,
        cur: cursor
    )

    if not result.ping():
        raise newException(NoConnectionError, "Failed to connect to the database.")


when isMainModule:
    let con = open(host="host", port=8889, catalog="hive", schema="dwh", username="benny")
    defer: con.close()
    var cur = con.cursor()
    cur.execute(sql"SELECT * FROM table LIMIT 1150")
    echo(cur.fetchAll(asTable=true))
