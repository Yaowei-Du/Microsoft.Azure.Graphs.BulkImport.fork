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
using System.Threading.Tasks;

namespace GraphBulkImportTest
{
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

            DocumentCollection collection = await client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(
                        ConfigurationManager.AppSettings["DocDBDatabase"],
                        ConfigurationManager.AppSettings["DocDBCollection"]))
                .ConfigureAwait(false);

            GraphBulkImport graphBulkImporter = new GraphBulkImport(client, collection, true);

            await graphBulkImporter.InitializeAsync();

            int count = 100;

            GraphBulkImportResponse vResponse =
                await graphBulkImporter.BulkImportVerticesAsync(GenerateVertices(count), false).ConfigureAwait(false);
            GraphBulkImportResponse eResponse =
                await graphBulkImporter.BulkImportEdgesAsync(GenerateEdges(count), false).ConfigureAwait(false);

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

                e.AddProperty("duration", i);

                yield return e;
            }
        }

        private static IEnumerable<Vertex> GenerateVertices(int count)
        {
            for (int i = 0; i < count; i++)
            {
                Vertex v = new Vertex(i.ToString(), "vertex");
                v.AddProperty(new VertexProperty("pk", i));
                v.AddProperty(new VertexProperty("name", "name" + i));

                yield return v;
            }
        }
    }
}
