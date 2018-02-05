import httpclient, base64, json, strutils, sequtils

type
  CursorAlreadyClosedException = object of Exception
  CursorAlreadyOpenedException = object of Exception
  OpenCursorInstanceNotFound = object of Exception
  NoMoreRowsLeftException = object of Exception

type
  PrestoClient = ref object of RootObj
    client:       HttpClient
    url:          string
    port:         string
    status:       Status
    timeout:      int
    cursorState:  CursorState
    curx:       Cursor
  
  Cursor = ref object
    client: PrestoClient
    query*: SqlQuery
    iteratorObj: ResultSet
  
  ResultSet = ref object
    activeIterator: bool
    iterInstance: iterator(): JsonNode


  CursorState {.pure.} = enum
    close = "CLOSE"
    open = "OPEN"

  SqlQuery = distinct string
  
  Status {.pure.} = enum
    finished  = "FINISHED",
    started   = "STARTED", 
    planning  = "PLANNING"

using
  self: PrestoClient
  cur: Cursor

proc newPrestoClient* (protocol, host, port, 
                       catalog, schema: string): PrestoClient =
  
  let client = newHttpClient()
  
  client.headers = newHttpHeaders({
    "X-Presto-Catalog": catalog,
    "X-Presto-Schema":  schema,
    "X-Presto-Source":  "nimPersto",
    "X-Presto-User":    "nimPresto"
  })

  return PrestoClient(
    client:  client,
    url:     "$1://$2:$3/v1/statement" % [protocol, host, port],
    timeout: 100,
    status:  Status.started,
    cursorState: CursorState.close

  )

proc newPrestoClient* (protocol, host, port, catalog,
                       schema, username, password: string): PrestoClient =
  
  let client = newHttpClient()
  
  client.headers = newHttpHeaders({
    "X-Presto-Catalog": catalog,
    "X-Presto-Schema":  schema,
    "X-Presto-Source":  "nimPersto",
    "X-Presto-User":    username,
    "Authorization":    "Basic " & encode(username & ":" & password)
  })
  
  return PrestoClient(
    client:  client,
    url:     "$1://$2:$3/v1/statement" % [protocol, host, port],
    timeout: 100,
    status:  Status.started,
    cursorState: CursorState.close
  )

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

proc syncProgress(self; data: JsonNode) =

  let state = data["stats"]["state"].str
  
  if state == "FINISHED" or not data.hasKey("nextUri"):
    self.status = Status.finished
  
  if data.hasKey("nextUri"):
    self.url = data["nextUri"].getStr
  
proc extractQueryData(responseDataChunk: JsonNode): seq[JsonNode] =
  responseDataChunk.getElems

iterator initiateQuery(self; query: SqlQuery): JsonNode =
  
  var i = 0
  var response = self.client.request(self.url, httpMethod = HttpPost, body = dbFormat(query))
  var respData = parseJson(response.body)
  
  while self.status != Status.finished:
    if i != 0:
      response = self.client.request(self.url, httpMethod = HttpGet, body = dbFormat(query))
      respData = parseJson(response.body)
    
    inc i
    
    if respData.hasKey("data"):
      for row in extractQueryData(respData["data"]):
        yield row

    self.syncProgress(respData)

proc execute*(cur; query: SqlQuery) =
  
  if cur.client.cursorState == CursorState.close:
    raise newException(OpenCursorInstanceNotFound, "Open Cursor not found please initialize it first.")

  cur.query = query

proc cursor*(self): Cursor =
  
  if self.cursorState == CursorState.open:
    raise newException(CursorAlreadyOpenedException, "Cursor is open.")
  
  self.cursorState = CursorState.open
  Cursor(
    client: self,
    iteratorObj: ResultSet(
      activeIterator: false
    )
  )

proc close*(self) =
  if self.cursorState == CursorState.open:
    self.cursorState = CursorState.close
  raise newException(CursorAlreadyClosedException, "Cursor is closed.")


proc fetchAll*(cur): seq[JsonNode] =
  
  if cur.client.cursorState == CursorState.close:
    raise newException(OpenCursorInstanceNotFound, "Open Cursor not found please initialize it first.")

  toSeq(cur.client.initiateQuery(cur.query))
    
proc fetchmany(cur): iterator(): JsonNode =
  
  if cur.client.cursorState == CursorState.close:
    raise newException(OpenCursorInstanceNotFound, "Open Cursor not found please initialize it first.")

  return iterator(): JsonNode =
    for r in cur.client.initiateQuery(cur.query):
      yield r

proc fetchMany*(cur; numOfRows: int): seq[JsonNode] =
  var data: seq[JsonNode] = @[]
  if not cur.iteratorObj.activeIterator:
    var iter = cur.fetchmany()
    cur.iteratorObj.activeIterator = true
    cur.iteratorObj.iterInstance = iter
  var rowsItered = 0
  while true:
    if rowsItered == numOfRows:
      return data
    let next = cur.iteratorObj.iterInstance()
    if not finished(cur.iteratorObj.iterInstance):
      data.add(next)
    else:
      break
    rowsItered.inc
  if data.len == 0:
    raise newException(NoMoreRowsLeftException, "No more rows left.")

proc fetchOne*(cur): seq[JsonNode] =
  return cur.fetchMany(1)
  
    
when isMainModule:
  let conn = newPrestoClient("https", "host", "8443", "hive", "default", "user", "password")
  var ctx = conn.cursor()
  ctx.execute(SqlQuery("select * from gett_algo.weather_forecast5d3h_v2 LIMIT 11"))


  


