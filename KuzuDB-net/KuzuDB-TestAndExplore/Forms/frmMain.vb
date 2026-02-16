'this simple app demonstrates the usage of Kuzu v.0.3.2

Imports System.Drawing.Design
Imports System.IO
Imports kuzunet


Public Class frmMain
    Private Sub frmMain_Load(sender As Object, e As EventArgs) Handles Me.Load

        If (System.IO.Directory.Exists("test")) Then
            System.IO.Directory.Delete("test", True)
        End If

        Using db As New kuzu_database
            Using conn As New kuzu_connection


                Dim state As kuzu_state
                Using config As kuzu_system_config = kuzu_default_system_config()
                    state = kuzu_database_init("test", config, db)
                    If state = kuzu_state.KuzuError Then
                        MessageBox.Show("Error creating DB")
                        Return
                    End If
                End Using ' Config

                state = kuzu_connection_init(db, conn)
                If state = kuzu_state.KuzuError Then
                    MessageBox.Show("Error connecting to DB")
                    Return
                End If

                PerformNonQuery(conn, "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))")
                PerformNonQuery(conn, "CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))")
                PerformNonQuery(conn, "CREATE REL TABLE Follows(FROM User TO User, since INT64)")
                PerformNonQuery(conn, "CREATE REL TABLE LivesIn(FROM User TO City)")

                PerformNonQuery(conn, "COPY User FROM ""csvs/users.csv""")
                PerformNonQuery(conn, "COPY City FROM ""csvs/cities.csv""")
                PerformNonQuery(conn, "COPY Follows FROM ""csvs/follows.csv""")
                PerformNonQuery(conn, "COPY LivesIn FROM ""csvs/lives-in.csv""")

                Using result As New kuzu_query_result

                    state = kuzu_connection_query(conn, "MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;", result)
                    If state = kuzu_state.KuzuError Then
                        MessageBox.Show("Error with MATCH query")
                        Return
                    End If


                    While (kuzu_query_result_has_next(result))
                        Dim name As String
                        Dim since As Long
                        Dim name2 As String

                        Using tuple As New kuzu_flat_tuple
                            kuzu_query_result_get_next(result, tuple)

                            Using value As New kuzu_value
                                kuzu_flat_tuple_get_value(tuple, 0, value)
                                kuzu_value_get_string(value, name)
                            End Using

                            Using value As New kuzu_value
                                kuzu_flat_tuple_get_value(tuple, 1, value)
                                kuzu_value_get_int64(value, since)
                            End Using

                            Using value As New kuzu_value
                                kuzu_flat_tuple_get_value(tuple, 2, value)
                                kuzu_value_get_string(value, name2)
                            End Using

                        End Using ' tuple

                        MessageBox.Show(String.Format("{0} follows {1} since {2}", name, name2, since))

                    End While

                End Using ' result

            End Using ' conn
        End Using ' db


        'Debugger.Break()

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

    Private Sub PerformNonQuery(ByRef conn As kuzu_connection, ByVal query As String)
        Using result As New kuzu_query_result
            Dim state As kuzu_state = kuzu_connection_query(conn, query, result)
            If state = kuzu_state.KuzuError Then
                MessageBox.Show("Error with query: " + query)
            End If
        End Using
    End Sub

End Class
