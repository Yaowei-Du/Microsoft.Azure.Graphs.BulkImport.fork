namespace GraphMigration
{
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;

    class Program
    {
        private static readonly string srcEndpoint = ConfigurationManager.AppSettings["SrcDocDBEndPoint"];
        private static readonly string srcAuthKey = ConfigurationManager.AppSettings["SrcDocDBKey"];
        private static readonly string srcDatabaseName = ConfigurationManager.AppSettings["SrcDocDBDatabase"];
        private static readonly string srcCollectionName = ConfigurationManager.AppSettings["SrcDocDBCollection"];

        private static readonly string destEndpoint = ConfigurationManager.AppSettings["DestDocDBEndPoint"];
        private static readonly string destAuthKey = ConfigurationManager.AppSettings["DestDocDBKey"];
        private static readonly string destDatabaseName = ConfigurationManager.AppSettings["DestDocDBDatabase"];
        private static readonly string destCollectionName = ConfigurationManager.AppSettings["DestDocDBCollection"];

        private static readonly Dictionary<string, object> idPKMapping = new Dictionary<string, object>();

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            RequestTimeout = new TimeSpan(1, 0, 0),
            MaxConnectionLimit = 1000,
            RetryOptions = new RetryOptions
            {
                MaxRetryAttemptsOnThrottledRequests = 10,
                MaxRetryWaitTimeInSeconds = 60
            }
        };

        static void Main(string[] args)
        {

            Task.Run(async () =>
            {
                DocumentClient srcClient = new DocumentClient(new Uri(Program.srcEndpoint), Program.srcAuthKey, Program.ConnectionPolicy);
                Uri srcCollectionLink = UriFactory.CreateDocumentCollectionUri(Program.srcDatabaseName, Program.srcCollectionName);
                DocumentCollection srcCollection = Program.ReadCollectionAsync(srcClient, srcDatabaseName, srcCollectionName, false).Result;

                DocumentClient destClient = new DocumentClient(new Uri(Program.destEndpoint), Program.destAuthKey, Program.ConnectionPolicy);
                Uri destCollectionLink = UriFactory.CreateDocumentCollectionUri(Program.destDatabaseName, Program.destCollectionName);
                DocumentCollection destCollection = Program.ReadCollectionAsync(destClient, destDatabaseName, destCollectionName, true).Result;

                Stopwatch watch = new Stopwatch();
                watch.Start();


                IBulkExecutor documentBulkImporter = new BulkExecutor(destClient, destCollection);
                await documentBulkImporter.InitializeAsync();

                BulkImportResponse bulkImportResponse = null;

                IEnumerable<Document> vertexdocs = GetDocs(srcClient, srcCollection, true);
                try
                {
                    bulkImportResponse = await documentBulkImporter.BulkImportAsync(
                        vertexdocs.Select(vertex => GetGraphelementString(vertex)),
                        enableUpsert:true);
                }
                catch (DocumentClientException de)
                {
                    Trace.TraceError("Document client exception: {0}", de);
                    throw;
                }
                catch (Exception e)
                {
                    Trace.TraceError("Exception: {0}", e);
                    throw;
                }

                IEnumerable<Document> edgeDocs = GetDocs(srcClient, srcCollection, false);
                try
                {
                    bulkImportResponse = await documentBulkImporter.BulkImportAsync(
                        edgeDocs.Select(edge => GetGraphelementString(edge)),
                        enableUpsert: true);
                }
                catch (DocumentClientException de)
                {
                    Trace.TraceError("Document client exception: {0}", de);
                    throw;
                }
                catch (Exception e)
                {
                    Trace.TraceError("Exception: {0}", e);
                    throw;
                }

                watch.Stop();

                Console.WriteLine("Time Taken: " + watch.ElapsedMilliseconds);

            }).GetAwaiter().GetResult();

            Console.WriteLine("Done, Please press any key to continue...");
            Console.ReadLine();
        }

        private static IEnumerable<Document> GetDocs(DocumentClient client, DocumentCollection collection, bool isVertices)
        {
            FeedOptions feedOptions = new FeedOptions
            {
                MaxItemCount = -1,
                MaxDegreeOfParallelism = -1,
                MaxBufferedItemCount = 100,
                EnableCrossPartitionQuery = true
            };

            string queryText;

            if (isVertices)
            {
                queryText = "select* from root where IS_DEFINED(root._isEdge) = false";
            }
            else
            {
                queryText = "select* from root where IS_DEFINED(root._isEdge) = true";
            }

            return client.CreateDocumentQuery<Document>(collection.AltLink, queryText, feedOptions).AsEnumerable();
        }

        private static string GetGraphelementString(Document doc)
        {
            if (doc.GetPropertyValue<bool>("_isEdge") != true)
            {
                // Elevating the partition key object as root level object for vertices
                JArray pk = doc.GetPropertyValue<JArray>(ConfigurationManager.AppSettings["DestPartitionKey"]);
                JObject prop = (JObject)pk[0];
                object pkObject = prop.GetValue("_value");

                doc.SetPropertyValue(ConfigurationManager.AppSettings["DestPartitionKey"], pkObject);

                if (!Program.idPKMapping.ContainsKey(doc.Id))
                {
                    idPKMapping.Add(doc.Id, pkObject);
                }
            }
            else
            {
                // Populating the source and destination partition key on the edges
                string srcId = doc.GetPropertyValue<string>("_vertexId");
                string sinkId = doc.GetPropertyValue<string>("_sink");

                doc.SetPropertyValue(ConfigurationManager.AppSettings["DestPartitionKey"], idPKMapping[srcId]);
                doc.SetPropertyValue("_sinkPartition", idPKMapping[sinkId]);
            }

            return doc.ToString();
        }

        private static async Task<DocumentCollection> ReadCollectionAsync(DocumentClient client, string databaseName, string collectionName, bool isPartitionedCollection)
        {
            if (Program.GetDatabaseIfExists(client, databaseName) == null)
            {
                return null;
            }

            DocumentCollection collection = null;

            try
            {
                collection = await client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(
                        databaseName,
                        collectionName)).ConfigureAwait(false);
            }
            catch (DocumentClientException)
            {
                throw;
            }

            return collection;
        }

        private static Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }
    }
}
