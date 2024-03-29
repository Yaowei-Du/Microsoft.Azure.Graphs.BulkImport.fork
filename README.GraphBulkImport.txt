Notes for using the Microsoft.Azure.Graphs.BulkImport library:

0. Please find the .nupkg for the library along with this read me. Make sure to reference all .dlls within this package, in your code.

1. This package comes with it's own internal version of the Microsoft.Azure.Documents.Client. Please don�t override that with one available on public Document SDK nuget.
We will get rid of this dependency after we release the next SDK.

2. If you are ingesting a lot of documents, higher RUs and a high memory VM is recommended. 

3. The API calls accept either an IEnumerable of Vertex or IEnumerable of Edges, so if you can do bulk import in a streaming fashion, no need to apply any batching logic.
Note that, while adding edges, we don�t check for the existence of the source or destination vertices. So one can create edges before the corresponding vertices, but the onus is on the user to make sure that they import the source and destination vertex eventually. 

4.  Settng useFlatProperty = true will create vertex properties as simple properties. Setting it false will create vertex properties as an array. Use the second model, only if you need support for multi-valued properties, i.e., one 
property can have multiple values. 

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

5. Take a look at the GraphBulkImportTest sample solution for an example usage.