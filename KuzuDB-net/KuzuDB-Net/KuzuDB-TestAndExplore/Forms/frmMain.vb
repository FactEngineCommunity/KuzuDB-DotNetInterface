Imports KuzuDB

Public Class frmMain
    Private Sub frmMain_Load(sender As Object, e As EventArgs) Handles Me.Load

        Dim lrKuzuDB As New KuzuDB.KuzuDB

        Dim databaseConfig As New KuzuDB.kuzu_connection()

        Debugger.Break()

        'Dim systemConfig As New SystemConfig(1UL << 31) ' set buffer manager size to 2GB
        'Dim database As New KuzuDB.kuzu_database(databaseConfig, systemConfig)

        '' Connect to the database
        'Dim connection As New Connection(database)

        '' Create the schema
        'connection.Query("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))")
        '' Load data
        'connection.Query("CREATE (u:User {name: 'Alice', age: 35})")
        '' Issue a query

        'Dim result As KuzuDB.kuzu_query_result = connection.Query("MATCH (a:User) RETURN a.name")
        '' Iterate over the result
        'While result.HasNext
        '    Dim row As Row = result.GetNext()
        '    Console.WriteLine(row.GetResultValue(0).GetStringVal())
        'End While

    End Sub

End Class
