# A few key concepts

1. [Cosmos DB Partitioning] (https://azure.microsoft.com/en-us/blog/10-things-to-know-about-documentdb-partitioned-collections/) : The same partitioning concepts applies to graph. 
2. Multi-valued vs single-valued properties: Cosmos db graph supports both the models. Multi-valued properties simply indicates that a vertex property can have multiple values. 
If you need this feature the BulkImporter needs to be configured by setting `useFlatProperty = false`

# Microsoft.Azure.Graphs.BulkImport Sample

> Note: The nuget package for the BulkImport utility has been updated on 12/12. In order to install it, you need to update Newtonsoft.json to version 10.0.1.

Notes for using the Microsoft.Azure.Graphs.BulkImport library:

0. Please find the `.nupkg` for the library in `./packages` along with this read me. Make sure to reference all .dlls within this package in your code.

1. This package comes with its own internal version of the Microsoft.Azure.Documents.Client. Please don’t override that with one available on public Document SDK nuget. We will update this dependency after we release the next SDK.

2. If you are ingesting a lot of documents, higher RUs and a high memory VM is recommended. 

3. The API calls accept either an `IEnumerable` of Vertex or `IEnumerable` of Edges, so if you want do bulk import in a streaming fashion, the recommendation is to create batches of Vertices and Edges, and call these APIs periodically.

Note that while adding edges we don’t check for the existence of the source or destination vertices. So one can create edges before the corresponding vertices, but it is on the user to make sure that they import the source and destination vertex eventually. 

4.  Setting `useFlatProperty = true` will create vertex properties as simple properties. Setting it false will create vertex properties as an array. Use the second model, only if you need support for multi-valued properties, i.e., one property can have multiple values. 
```csharp
/// <summary>
/// Initializes a new instance of the <see cref="GraphBulkImport"/> class.
/// </summary>
/// <param name="client">The DocumentDB client instance.</param>
/// <param name="documentCollection">The document collection to which documents are to be bulk imported.</param>
/// <param name="useFlatProperty">Whether the graph is set up to use Flat vertex property. If not the graph will use Gremlin vertex property which supports multi-valued and meta properties</param>
public GraphBulkImport(DocumentClient client, DocumentCollection documentCollection, bool useFlatProperty)
{
    //TODO: We are making the useFlatProperty field to be available in GraphConnection, once we have that we will remove this 

    this.client = client;
    this.collection = documentCollection;
    this.useFlatProperty = useFlatProperty;
}
```
5. Both BulkImportVerticesAsync() and BulkImportEdgesAsync() support upsert mode through the usage of a boolean parameter. If you enable this flag, this will let you replace a vertex/edge if they are already present. 
Whether a vertex/edge is already presenet is determined by whether there already exist a vertex/edge with same id (or same [id, partitionkey] pair for a partitioned collection).

Note that, if you have the enableUpsert = false, trying to add vertes/edge with existing id (or, [id, partitionkey] pair) will throw an exception. On the other hand doing the same thing with enableUpsert = true
will replace the vertex/edge. 

So, these need to handled carefully. With, enableUpsert = true, there is no way for the tool to know whether the original intention is to UpSert the vertex or it was due to an error in the application logic that
generated two vertices with same id (or [id, partitionkey] pair). 

6. Take a look at the GraphBulkImportTest sample solution for an example usage.

Example appsettings:
```csharp
  <appSettings>
    <add key="DocDBEndPoint" value="https://****.documents.azure.com:443/" />
    <add key="DocDBKey" value="zprRQ7PIxOnNZ85cQaA8ztC2I3IHO3zIcnryJUQy8o9ygfhAgOPpsnDyiBcz7zFWefweqmnSXXMZGf5S1X866g==" />
    <add key="DocDBDatabase" value="graphdb" />
    <add key="DocDBCollection" value="graphcollection" />
    <add key="PartitionKeyName" value="pk" />
  </appSettings>
```

# BenchMark

Database location: West US
Client location: Local machine@ West US
Client Configuration: 
Number of vertex properties: 2
Number of edge properties: 1

| collection Type  | RUs provisioned | #Vertices | #Edges | Total time(s) | Writes/s | Average RU/s | Average RU/insert
| ------------- | ------------- | ------------- | ------------- | ------------- |------------- | ------------- | ------------- |
| Fixed (10GB)  | 10,000  | 200K | ~200K | 207.28 | 1930 | 10184 | 10.55 |
| unlimited (100GB)  | 100,000  | 200K | ~200K | 21.28 | 18679 | 83019 | 8.88 |
| Unlimited (830GB)  | 500,000  | 200K | ~200K | 9.63 | 41495 | 163019 | 12.70 |


# Troubleshooting

1. Slow Ingestion rate: 
	- Check the distance between the client location and the Azure region where the database is hosted. 
	- Check the configured throughput, ingestion can be slow if the tool is getting [throttled] (https://docs.microsoft.com/en-us/azure/cosmos-db/request-units).  It is recommended that you increase the RU/s 
during ingestion and then scale it down later. This can be done programmatically via the [ReplaceOfferAsync() API] (https://docs.microsoft.com/en-us/azure/cosmos-db/set-throughput). 
	- Use a client with high memory, otherwise GC pressure might interrupt the ingestion. 
	- Turn server GC on. 
	- Do you have fixed collection (10GB)? Ingestion can be a bit slower for such collection compared to partitioned collection. Ingestion to a partitioned collection is faster as multiple partitions can be 
filled in parallel, while a single partition is filled in a serial fashion. If you need even faster ingestion for fixed collection, you can partition your data locally and make multiple parallel calls to the
bulk import API.  

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
