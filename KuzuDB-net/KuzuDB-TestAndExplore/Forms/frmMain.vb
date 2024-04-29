'this simple app demonstrates the usage of Kuzu v.0.3.2

Imports kuzunet

Public Class frmMain
    Private Sub frmMain_Load(sender As Object, e As EventArgs) Handles Me.Load

        Dim config As kuzu_system_config = kuzu_default_system_config()

        Dim db As kuzu_database = kuzu_database_init("test", config)
        Dim conn As kuzu_connection = kuzu_connection_init(db)

        Dim result As kuzu_query_result = kuzu_connection_query(conn, "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))")
        kuzu_query_result_destroy(result)
        result = kuzu_connection_query(conn, "CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))")
        kuzu_query_result_destroy(result)
        result = kuzu_connection_query(conn, "CREATE REL TABLE Follows(FROM User TO User, since INT64)")
        kuzu_query_result_destroy(result)
        result = kuzu_connection_query(conn, "CREATE REL TABLE LivesIn(FROM User TO City)")
        kuzu_query_result_destroy(result)

        result = kuzu_connection_query(conn, "COPY User FROM ""csvs/users.csv""")
        kuzu_query_result_destroy(result)
        result = kuzu_connection_query(conn, "COPY City FROM ""csvs/cities.csv""")
        kuzu_query_result_destroy(result)
        result = kuzu_connection_query(conn, "COPY Follows FROM ""csvs/follows.csv""")
        kuzu_query_result_destroy(result)
        result = kuzu_connection_query(conn, "COPY LivesIn FROM ""csvs/lives_in.csv""")
        kuzu_query_result_destroy(result)

        result = kuzu_connection_query(conn, "MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;")

        While (kuzu_query_result_has_next(result))
            Dim tuple As kuzu_flat_tuple = kuzu_query_result_get_next(result)

            Dim value As kuzu_value = kuzu_flat_tuple_get_value(tuple, 0)
            Dim name As String = kuzu_value_get_string(value)
            kuzu_value_destroy(value)

            value = kuzu_flat_tuple_get_value(tuple, 1)
            Dim since As Long = kuzu_value_get_int64(value)
            kuzu_value_destroy(value)

            value = kuzu_flat_tuple_get_value(tuple, 2)
            Dim name2 As String = kuzu_value_get_string(value)
            kuzu_value_destroy(value)

            MessageBox.Show(String.Format("{0} follows {1} since {2}", name, name2, since))
            kuzu_flat_tuple_destroy(tuple)
        End While


        kuzu_query_result_destroy(result)
        kuzu_connection_destroy(conn)
        kuzu_database_destroy(db)

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
