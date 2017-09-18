import tables
import strutils
import unittest
import ../db_presto

suite "Should return as we expected by giving values.":
    const expectedOutput: seq[seq[string]] = @[@["Shoki", "Zlodnik", "0980102943"],
                                                @["Roman", "Jorgionik", "678929372"],
                                                @["David", "Alfonso", "987898776"],
                                                @["Moshe", "Moshe", "876543210"],
                                                @["Benny", "Elgazar", "012345678"]]
    const expectedTableOutput: seq[Table[string, string]] = @[@{"lastname": "Zlodnik", "name": "Shoki", "phonenumber": "0980102943"}.toTable,
                                                            @{"lastname": "Jorgionik", "name": "Roman", "phonenumber": "678929372"}.toTable, 
                                                            @{"lastname": "Alfonso", "name": "David", "phonenumber": "987898776"}.toTable,
                                                            @{"lastname": "Moshe", "name": "Moshe", "phonenumber": "876543210"}.toTable,
                                                            @{"lastname": "Elgazar", "name": "Benny", "phonenumber": "012345678"}.toTable]

    const expectedFetchManyOutput: seq[seq[string]] = @[@["Shoki", "Zlodnik", "0980102943"], @["Roman", "Jorgionik", "678929372"]]
    
    const expectedFetchManyTableOutput: seq[Table[string, string]] = @[@{"lastname": "Zlodnik", "name": "Shoki", "phonenumber": "0980102943"}.toTable,
                                                                        @{"lastname": "Jorgionik", "name": "Roman", "phonenumber": "678929372"}.toTable]

    const expectedFetchOneOutput: seq[string] = @["Shoki", "Zlodnik", "0980102943"]

    const expectedFetchOneTableOutput: Table[string, string] = @{"lastname": "Zlodnik", "name": "Shoki", "phonenumber": "0980102943"}.toTable

    let con = open(host="localhost", port=6969, catalog="mysql", schema="test", username="benny")
    defer: con.close()
    
    test "When fetch all Should get the out put equal to the expected output.":
        let cur = con.cursor()
        cur.execute(sql"SELECT * FROM testTable")
        var data = cur.fetchAll(asTable=false)
        check(data.len == expectedOutput.len)
        check(data == expectedOutput)
    
    test "When fetch all with asTable set to true, it should return a table Like results sets":
        let cur = con.cursor()
        cur.execute(sql"SELECT * FROM testTable")
        var data = cur.fetchAll(asTable=true)
        check(data.len == expectedOutput.len)
        check(data == expectedTableOutput)
    
    test "When fetchMany used it should return the amount set inside the fetchMany":
        let cur = con.cursor()
        cur.execute(sql"SELECT * FROM testTable")
        var data = cur.fetchMany(2, asTable=false)
        check(data.len == 2)
        check(data == expectedFetchManyOutput)
    
    test "When fetchMany used with asTable set to true it should return the results in table format like": 
        let cur = con.cursor()
        cur.execute(sql"SELECT * FROM testTable")
        var data = cur.fetchMany(2, asTable=true)
        check(data.len == 2)
        check(data == expectedFetchManyTableOutput)
    
    test "When fetchOne used it should return 1 result as a time":
        let cur = con.cursor()
        cur.execute(sql"SELECT * FROM testTable")
        var data = cur.fetchOne(asTable=false)
        check(data == expectedFetchOneOutput)
    
    test "when fetchOne used with asTable set to true it should return one result and as Table format like":
        let cur = con.cursor()
        cur.execute(sql"SELECT * FROM testTable")
        var data = cur.fetchOne(asTable=true)
        check(data == expectedFetchOneTableOutput)
    

