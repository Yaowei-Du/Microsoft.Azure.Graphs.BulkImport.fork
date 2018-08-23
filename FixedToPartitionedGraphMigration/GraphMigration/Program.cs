//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace GraphMigration
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.CosmosDB.BulkExecutor.Graph;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json.Linq;
    using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;

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


                IBulkExecutor documentBulkImporter = new GraphBulkExecutor(destClient, destCollection);
                await documentBulkImporter.InitializeAsync();

                BulkImportResponse bulkImportResponse = null;

                IEnumerable<JObject> vertexdocs = GetDocs(srcClient, srcCollection, true);
                try
                {
                    bulkImportResponse = await documentBulkImporter.BulkImportAsync(
                        vertexdocs.Select(vertex => ConvertToGremlinVertex(vertex)),
                        enableUpsert:true,
                        maxInMemorySortingBatchSize: 100000);
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

                Console.WriteLine("Importing edges");

                IEnumerable<JObject> edgeDocs = GetDocs(srcClient, srcCollection, false);
                try
                {
                    bulkImportResponse = await documentBulkImporter.BulkImportAsync(
                        edgeDocs.Select(edge => ConvertToGremlinEdge(edge)),
                        enableUpsert: true,
                        maxInMemorySortingBatchSize: 100000);
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

        private static IEnumerable<JObject> GetDocs(DocumentClient client, DocumentCollection collection, bool isVertices)
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

            return client.CreateDocumentQuery<JObject>(collection.AltLink, queryText, feedOptions).AsEnumerable();
        }

        private static GremlinVertex ConvertToGremlinVertex(JObject doc)
        {
            GremlinVertex gv = null;

            if (doc.GetValue("_isEdge") == null)
            {
                string docId = (string)doc.GetValue("id");
                // add id 
                // add label
                gv = new GremlinVertex(docId, (string)doc.GetValue("label"));

                // add pk : Elevating the partition key object as root level object for vertices
                JArray pk = (JArray)doc.GetValue(ConfigurationManager.AppSettings["DestPartitionKey"]);

                if (pk.Count > 1)
                {
                    throw new Exception("Partition key property can't be multi-valued property");
                }

                JObject prop = (JObject)pk[0];
                object pkObject = prop.GetValue("_value");

                gv.AddProperty(new GremlinVertexProperty(ConfigurationManager.AppSettings["DestPartitionKey"], pkObject));

                if (!Program.idPKMapping.ContainsKey(docId))
                {
                    idPKMapping.Add(docId, pkObject);
                }

                // add ttl 
                if (doc.GetValue("ttl") != null)
                {
                    gv.AddProperty(new GremlinVertexProperty("ttl", (long)doc.GetValue("ttl")));
                }

                // add all properties

                foreach (JProperty jp in doc.Properties())
                {
                    if(jp.Name == "id" 
                        || jp.Name == ConfigurationManager.AppSettings["DestPartitionKey"]
                        || jp.Name == "ttl"
                        || jp.Name == "label"
                        || jp.Name == "_rid"
                        || jp.Name == "_etag"
                        || jp.Name == "_self"
                        || jp.Name == "_ts"
                        || jp.Name == "_attachements"
                        || !(jp.Value is JArray))
                    {
                        continue;
                    }

                    JArray propArray = jp.Value as JArray;
                    JObject propObject = (JObject)propArray[0];
                    object propertValue = propObject.GetValue("_value");

                    gv.AddProperty(new GremlinVertexProperty(jp.Name, propertValue));
                }
            }

            return gv;
        }

        private static GremlinEdge ConvertToGremlinEdge(JObject doc)
        {
            GremlinEdge ge = null;

            if ((bool)doc.GetValue("_isEdge") == true)
            {
                // Populating the source and destination partition key on the edges
                string outVertexId = (string)doc.GetValue("_vertexId");
                string invertexId = (string)doc.GetValue("_sink");

                string edgeId = (string)doc.GetValue("id");
                string edgeLabel = (string)doc.GetValue("label");

                string outVertexLabel = (string)doc.GetValue("_vertexLabel");
                string invertexLabel = (string)doc.GetValue("_sinkLabel");

                object outVertexPartitionKey = idPKMapping[outVertexId];
                object inVertexPartitionKey = idPKMapping[invertexId];

                ge = new GremlinEdge(
                    edgeId: edgeId,
                    edgeLabel: edgeLabel,
                    outVertexId: outVertexId,
                    inVertexId: invertexId,
                    outVertexLabel: outVertexLabel,
                    inVertexLabel: invertexLabel,
                    outVertexPartitionKey: outVertexPartitionKey,
                    inVertexPartitionKey: inVertexPartitionKey);
            }

            return ge;
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
