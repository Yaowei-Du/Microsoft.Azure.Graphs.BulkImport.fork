namespace GraphBulkImportTest
{
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Graphs.BulkImport;
    using Microsoft.Azure.Graphs.Elements;
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

        public static async Task TestBulkInSertAsync()
        {
            DocumentClient client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["DocDBEndPoint"]),
                ConfigurationManager.AppSettings["DocDBKey"],
                new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });

            string databaseId = ConfigurationManager.AppSettings["DocDBDatabase"];
            string collectionId = ConfigurationManager.AppSettings["DocDBCollection"];

            // Uncomment the section below to create collection programatically

            //Database database =
            //    client.CreateDatabaseQuery()
            //        .Where(db => db.Id == databaseId)
            //        .AsEnumerable()
            //        .FirstOrDefault();

            //string partitionKey = ConfigurationManager.AppSettings["PartitionKeyName"];
            //DocumentCollection myCollection = new DocumentCollection
            //{
            //    Id = collectionId
            //};

            //if (!string.IsNullOrWhiteSpace(partitionKey))
            //{
            //    myCollection.PartitionKey.Paths.Add("/" + partitionKey); // Omit this if you need a fixed collection 
            //}

            //await client.CreateDocumentCollectionAsync(
            //    database.SelfLink,
            //    myCollection,
            //    new RequestOptions { OfferThroughput = 500000 });

            DocumentCollection collection = await client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(
                        databaseId,
                        collectionId))
                .ConfigureAwait(false);

            GraphBulkImport graphBulkImporter = new GraphBulkImport(client, collection, true);

            await graphBulkImporter.InitializeAsync();

            int count = int.Parse(ConfigurationManager.AppSettings["NumVertices"]);

            IEnumerable<Vertex> iVertices = GenerateVertices(count);
            GraphBulkImportResponse vResponse =
                await graphBulkImporter.BulkImportVerticesAsync(iVertices, enableUpsert:true).ConfigureAwait(false);
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

            Console.ReadLine();
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
                v.AddProperty(new VertexProperty("pk", i));
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
