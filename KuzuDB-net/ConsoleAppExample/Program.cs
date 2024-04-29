// this simple console app demonstrates the usage of Kuzu v.0.3.2

using System;
using static kuzunet;

namespace ConsoleAppExample
{
    internal class Program
    {
        static void Main(string[] args)
        {
            kuzu_database db = kuzu_database_init("test", kuzu_default_system_config());
            kuzu_connection conn = kuzu_connection_init(db);

            kuzu_query_result result;

            result = kuzu_connection_query(conn, "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))");
            kuzu_query_result_destroy(result);
            result = kuzu_connection_query(conn, "CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))");
            kuzu_query_result_destroy(result);
            result = kuzu_connection_query(conn, "CREATE REL TABLE Follows(FROM User TO User, since INT64)");
            kuzu_query_result_destroy(result);
            result = kuzu_connection_query(conn, "CREATE REL TABLE LivesIn(FROM User TO City)");
            kuzu_query_result_destroy(result);

            result = kuzu_connection_query(conn, "COPY User FROM \"csv/users.csv\"");
            kuzu_query_result_destroy(result);
            result = kuzu_connection_query(conn, "COPY City FROM \"csv/cities.csv\"");
            kuzu_query_result_destroy(result);
            result = kuzu_connection_query(conn, "COPY Follows FROM \"csv/follows.csv\"");
            kuzu_query_result_destroy(result);
            result = kuzu_connection_query(conn, "COPY LivesIn FROM \"csv/lives.csv\"");
            kuzu_query_result_destroy(result);

            result = kuzu_connection_query(conn, "MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;");

            while (kuzu_query_result_has_next(result))
            {

                kuzu_flat_tuple tuple = kuzu_query_result_get_next(result);
                kuzu_value value;

                value = kuzu_flat_tuple_get_value(tuple, 0);
                String name = kuzu_value_get_string(value);
                kuzu_value_destroy(value);

                value = kuzu_flat_tuple_get_value(tuple, 1);
                String since = kuzu_value_get_string(value);
                kuzu_value_destroy(value);

                value = kuzu_flat_tuple_get_value(tuple, 2);
                String name2 = kuzu_value_get_string(value);
                kuzu_value_destroy(value);

                Console.WriteLine(String.Format("{0} follows {1} since {2}", name, name2, since));
                kuzu_flat_tuple_destroy(tuple);
            }
            kuzu_query_result_destroy(result);
            kuzu_connection_destroy(conn);
            kuzu_database_destroy(db);

            Console.WriteLine("Press enter to close...");
            Console.ReadLine();
        }
    }
}
