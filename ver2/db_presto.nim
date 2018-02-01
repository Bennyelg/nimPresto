import httpclient, base64, json, strutils, sequtils

type
  
  PrestoClient = ref object of RootObj
    client:    HttpClient
    url:       string
    port:      string
    status:    Status
    timeout:   int
  
  SqlQuery = distinct string
  
  Status {.pure.} = enum
    finished  = "FINISHED",
    started   = "STARTED", 
    planning  = "PLANNING"

using
  self: PrestoClient

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
    status:  Status.started
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
    status:  Status.started
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

iterator execute(self; query: SqlQuery): JsonNode =
  
  var i = 0
  var response = self.client.request(self.url, httpMethod = HttpPost, body = dbFormat(query))
  var respData = parseJson(response.body)
  
  self.syncProgress(respData)
  
  while self.status != Status.finished:
    
    if i != 0:
      response = self.client.request(self.url, httpMethod = HttpGet, body = dbFormat(query))
      respData = parseJson(response.body)
    
    i += 1
    
    if respData.hasKey("data"):
      for row in extractQueryData(respData["data"]):
        yield row

      #   self.columns = respData["columns"].mapIt(it["name"].str)

    self.syncProgress(respData)


when isMainModule:
  let prestoConnection = newPrestoClient("https", "host", "8443", "hive", "default", "user", "pass")
  for r in prestoConnection.execute(SqlQuery("SELECT * from default.live_events limit 5")):
    echo(r)
  


