namespace GraphBulkImportTest
{
    using Gremlin.Net;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.Graphs;
    using Microsoft.Azure.Graphs.BulkImport;
    using Microsoft.Azure.Graphs.Elements;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            Program.TestBulkInSertAsync().Wait();           
        }

        public static List<string> gremlinQueries = new List<string>
        {
            "g.V().count()", // Counting number of vertices
            "g.E().count()", // Counting number of edges
            "g.V(\'1\')",
        };

        public static List<string> documentQueries = new List<string>
        {
            "SELECT VALUE COUNT(1) from root where IS_DEFINED(root._isEdge) = false", // Counting number of vertices
            "SELECT VALUE COUNT(1) from root where root._isEdge = true", // Counting number of edges
        };

        public static async Task TestBulkInSertAsync()
        {
            DocumentClient client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["DocumentServerEndPoint"]),
                ConfigurationManager.AppSettings["PrimaryKey"],
                new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });

            string databaseId = ConfigurationManager.AppSettings["Database"];
            string collectionId = ConfigurationManager.AppSettings["Collection"];

            Database database =
                client.CreateDatabaseQuery()
                    .Where(db => db.Id == databaseId)
                    .AsEnumerable()
                    .FirstOrDefault();

            DocumentCollection collection = null;

            try
            {
                collection = await client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());

                string partitionKey = ConfigurationManager.AppSettings["PartitionKeyName"];
                int throughput = int.Parse(ConfigurationManager.AppSettings["Throughput"]);
                bool isPartitionedGraph = bool.Parse(ConfigurationManager.AppSettings["IsPartitionedGraph"]);

                Console.WriteLine(string.Format("No graph found. Creating a graph collection: {0} with throughput = {1}", collectionId, throughput));
                if (isPartitionedGraph)
                {
                    Console.WriteLine(string.Format("The collection is a partitioned collection with partition Key: /{0}", partitionKey));
                }
                else
                {
                    Console.WriteLine($"The collection is a fixed collection with no partition Key");
                }
                Console.WriteLine("Press any key to continue ...");
                Console.ReadKey();

                DocumentCollection myCollection = new DocumentCollection
                {
                    Id = collectionId
                };

                if (isPartitionedGraph)
                {
                    if (string.IsNullOrWhiteSpace(partitionKey))
                    {
                        throw new ArgumentNullException("PartionKey can't be null for a partitioned collection");
                    }

                    myCollection.PartitionKey.Paths.Add("/" + partitionKey);
                }

                collection = await client.CreateDocumentCollectionAsync(
                    database.SelfLink,
                    myCollection,
                    new RequestOptions { OfferThroughput = throughput });
            }


            GraphBulkImport graphBulkImporter = new GraphBulkImport(client, collection, useFlatProperty: false);

            await graphBulkImporter.InitializeAsync();

            int count = int.Parse(ConfigurationManager.AppSettings["NumVertices"]);

            GraphBulkImportResponse vResponse =
                await graphBulkImporter.BulkImportVerticesAsync(GenerateVertices(count), enableUpsert: true).ConfigureAwait(false);
            GraphBulkImportResponse eResponse =
                await graphBulkImporter.BulkImportEdgesAsync(GenerateEdges(count), enableUpsert: true).ConfigureAwait(false);

            Console.WriteLine("\nSummary for batch");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(
                "Inserted {0} vertices @ {1} edges @ {2} writes/s, {3} RU/s in {4} sec)",
                vResponse.NumberOfVerticesImported,
                eResponse.NumberOfEdgesImported,
                Math.Round(
                    (vResponse.NumberOfVerticesImported + eResponse.NumberOfEdgesImported) /
                    (vResponse.TotalTimeTaken.TotalSeconds + eResponse.TotalTimeTaken.TotalSeconds)),
                Math.Round(
                    (vResponse.TotalRequestUnitsConsumed + eResponse.TotalRequestUnitsConsumed) /
                    (vResponse.TotalTimeTaken.TotalSeconds + eResponse.TotalTimeTaken.TotalSeconds)),
                vResponse.TotalTimeTaken.TotalSeconds + eResponse.TotalTimeTaken.TotalSeconds);
            Console.WriteLine(
                "Average RU consumption per document: {0}",
                (vResponse.TotalRequestUnitsConsumed + eResponse.TotalRequestUnitsConsumed) /
                (vResponse.NumberOfVerticesImported + eResponse.NumberOfVerticesImported));
            Console.WriteLine("---------------------------------------------------------------------\n ");

            // Using the gremlin server (Supports gremlin queries)
            try
            {
                GremlinServer server = new GremlinServer(
                    ConfigurationManager.AppSettings["GremlinServerEndPoint"],
                    int.Parse(ConfigurationManager.AppSettings["GremlinServerPort"]),
                    true,
                    "/dbs/" + ConfigurationManager.AppSettings["Database"] + "/colls/" + ConfigurationManager.AppSettings["Collection"],
                    ConfigurationManager.AppSettings["PrimaryKey"]);

                using (GremlinClient gClient = new GremlinClient(server))
                {
                    foreach (string query in Program.gremlinQueries)
                    {
                        ExecuteGremlinServerQuery(gClient, query);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            Console.WriteLine("---------------------------------------------------------------------\n ");

            // Using the Graph Server (Supports gremlin queries)
            try
            {
                foreach (string query in Program.gremlinQueries)
                {
                    await ExecuteGraphServerQueryAsync(client, collection, query);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            Console.WriteLine("---------------------------------------------------------------------\n ");

            // Using the document server (Supports DocumentDB SQL queries)
            try
            {
                foreach (string query in Program.documentQueries)
                {
                    await ExecuteDocumentServerQueryAsync(client, collection, query);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            Console.WriteLine("---------------------------------------------------------------------\n ");

            Console.ReadLine();
        }

        private static async Task ExecuteGraphServerQueryAsync(DocumentClient client, DocumentCollection collection, string queryString)
        {
            IDocumentQuery<dynamic> query = client.CreateGremlinQuery<dynamic>(collection, queryString);
            while (query.HasMoreResults)
            {
                foreach (dynamic result in await query.ExecuteNextAsync())
                {
                    Console.WriteLine(result);
                }
            }
        }

        private static async Task ExecuteDocumentServerQueryAsync(DocumentClient client, DocumentCollection collection, string queryString)
        {
            FeedOptions options = new FeedOptions { MaxItemCount = 1, EnableCrossPartitionQuery = bool.Parse(ConfigurationManager.AppSettings["IsPartitionedGraph"])};
            var documentQuery = client.CreateDocumentQuery<dynamic>(collection.SelfLink, queryString, options).AsDocumentQuery();
            while (documentQuery.HasMoreResults)
            {
                foreach (dynamic result in await documentQuery.ExecuteNextAsync())
                {
                    Console.WriteLine($"\t {JsonConvert.SerializeObject(result)}");
                }
            }
        }

        private static void ExecuteGremlinServerQuery(GremlinClient gClient, string query)
        {
           IEnumerable<dynamic> result =
                        GremlinClientExtensions.SubmitAsync<dynamic>(gClient, requestScript: query).Result;

            Console.WriteLine(result.First().ToString());
        }

        private static IEnumerable<Edge> GenerateEdges(int count)
        {
            StringBuilder propertyName = new StringBuilder();

            for (int i = 0; i < count - 1; i++)
            {
                Edge e = new Edge(
                    "e" + i,
                    "knows",
                    i.ToString(),
                    (i + 1).ToString(),
                    "vertex",
                    "vertex",
                    i,
                    i + 1);

                for (int j = 0; j < 5; j++)
                {
                    propertyName.Append("property");
                    propertyName.Append(j);
                    e.AddProperty(propertyName.ToString(), "dummyvalue");
                    propertyName.Clear();
                }

                yield return e;
            }
        }

        private static IEnumerable<Vertex> GenerateVertices(int count)
        {
            StringBuilder propertyName = new StringBuilder();

            for (int i = 0; i < count; i++)
            {
                Vertex v = new Vertex(i.ToString(), "vertex");
                v.AddProperty(new VertexProperty(ConfigurationManager.AppSettings["PartitionKeyName"], i));
                v.AddProperty(new VertexProperty("name", "name" + i));

                for (int j = 0; j < 10; j++)
                {
                    propertyName.Append("property");
                    propertyName.Append(j);
                    v.AddProperty(new VertexProperty(propertyName.ToString(), "dummyvalue"));
                    propertyName.Clear();
                }

                yield return v;
            }
        }

    }
}
