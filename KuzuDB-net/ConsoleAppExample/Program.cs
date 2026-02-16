// this simple console app demonstrates the usage of Kuzu v.0.8.2

using System;
using static kuzunet;

namespace ConsoleAppExample
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Wipe out old test DB if it exists
            if (System.IO.Directory.Exists("test"))
                System.IO.Directory.Delete("test", true);

            using (kuzu_database db = new kuzu_database())
            using (kuzu_connection conn = new kuzu_connection())
            {
                kuzu_state state;

                using (kuzu_system_config config = kuzu_default_system_config())
                { 
                    state = kuzu_database_init("test", config, db);
                    if (state == kuzu_state.KuzuError)
                    {
                        Console.WriteLine("Could not create DB");
                        return;
                    }
                }

                state = kuzu_connection_init(db, conn);
                if (state == kuzu_state.KuzuError)
                {
                    Console.WriteLine("Could not connect to DB");
                    return;
                }

                PerformNonQuery(conn, "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))");
                PerformNonQuery(conn, "CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))");
                PerformNonQuery(conn, "CREATE REL TABLE Follows(FROM User TO User, since INT64)");
                PerformNonQuery(conn, "CREATE REL TABLE LivesIn(FROM User TO City)");

                PerformNonQuery(conn, "COPY User FROM \"csv/users.csv\"");
                PerformNonQuery(conn, "COPY City FROM \"csv/cities.csv\"");
                PerformNonQuery(conn, "COPY Follows FROM \"csv/follows.csv\"");
                PerformNonQuery(conn, "COPY LivesIn FROM \"csv/lives-in.csv\"");

                using (var result = new kuzu_query_result())
                {
                    state = kuzu_connection_query(conn, "MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;", result);

                    while (kuzu_query_result_has_next(result))
                    {
                        string name, name2;
                        long since;

                        using (var tuple = new kuzu_flat_tuple())
                        {
                            kuzu_query_result_get_next(result, tuple);

                            using (var value = new kuzu_value())
                            {
                                kuzu_flat_tuple_get_value(tuple, 0, value);
                                kuzu_value_get_string(value, out name);
                            }

                            using (var value = new kuzu_value())
                            {
                                kuzu_flat_tuple_get_value(tuple, 1, value);
                                kuzu_value_get_int64(value, out since);
                            }

                            using (var value = new kuzu_value())
                            {
                                kuzu_flat_tuple_get_value(tuple, 2, value);
                                kuzu_value_get_string(value, out name2);
                            }
                        }

                        Console.WriteLine(String.Format("{0} follows {1} since {2}", name, name2, since));
                    }
                }
            }

            Console.WriteLine("Press enter to close...");
            Console.ReadLine();
        }

        private static void PerformNonQuery(kuzu_connection conn, string query)
        {
            using (var result = new kuzu_query_result())
            {
                var state = kuzu_connection_query(conn, query, result);
                if (state == kuzu_state.KuzuError)
                {
                    Console.WriteLine("Could not perform: " + query);
                    return;
                }
            }
        }
    }
}
