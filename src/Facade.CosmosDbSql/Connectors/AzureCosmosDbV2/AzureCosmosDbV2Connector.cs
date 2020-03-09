using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Connector.CosmosDbSql.Documents;
using Connector.CosmosDbSql.Enums;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Connector.CosmosDbSql.Connectors.AzureCosmosDbV2
{
    public class AzureCosmosDbV2Connector : IAzureCosmosDbV2Connector, IDisposable
    {
        private readonly DocumentClient _client;

        public AzureCosmosDbV2Connector(string endpoint, string key)
        {
            _client = new DocumentClient(new Uri(endpoint), key);
        }

        public async Task<bool> CreateDatabaseIfNotExistsAsync(
            string databaseId,
            int? sharedThroughput,
            CosmosConsistencyLevel cosmosConsistencyLevel = CosmosConsistencyLevel.Strong)
        {
            var options = GetRequestOptions(partitionKey: null, throughput: null, sharedThroughput, cosmosConsistencyLevel);
            var database = new Database { Id = databaseId };

            var response = await _client.CreateDatabaseIfNotExistsAsync(database, options);
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> CreateCollectionIfNotExistsAsync(
            string databaseId,
            string collectionId,
            string partitionKey,
            CosmosConsistencyLevel cosmosConsistencyLevel = CosmosConsistencyLevel.Strong,
            CosmosIndexingMode cosmosIndexingMode = CosmosIndexingMode.Consistent)
        {
            var uri = UriFactory.CreateDatabaseUri(databaseId);
            var requestOptions = GetRequestOptions(partitionKey, throughput: null, sharedThroughput: null, cosmosConsistencyLevel);
            var indexingMode = GetIndexingMode(cosmosIndexingMode);
            var documentCollection = GetDocumentCollection(collectionId, partitionKey, indexingMode);

            var response = await _client.CreateDocumentCollectionIfNotExistsAsync(uri, documentCollection, requestOptions);
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> CreateItemAsync<T>(string databaseId, string collectionId, DocumentBase<T> item)
        {
            var uri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            var document = await _client.CreateDocumentAsync(uri, item, null, true);
            return IsResponseValid(document.StatusCode);
        }

        public async Task<bool> UpdateItemAsync<T>(string databaseId, string collectionId, DocumentBase<T> item)
        {
            var uri = UriFactory.CreateDocumentUri(databaseId, collectionId, item.Id);
            var response = await _client.ReplaceDocumentAsync(uri, item);
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> UpsertItemAsync<T>(string databaseId, string collectionId, DocumentBase<T> item)
        {
            var uri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            var document = await _client.UpsertDocumentAsync(uri, item, null, true);
            return IsResponseValid(document.StatusCode);
        }

        public async Task<bool> DeleteItemAsync<T>(string databaseId, string collectionId, string id, string partitionKey = null)
        {
            var uri = UriFactory.CreateDocumentUri(databaseId, collectionId, id);
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                var response = await _client.DeleteDocumentAsync(uri);
                return IsResponseValid(response.StatusCode);
            }
            else
            {
                var options = new RequestOptions
                {
                    PartitionKey = new PartitionKey(partitionKey)
                };
                var response = await _client.DeleteDocumentAsync(uri, options);
                return IsResponseValid(response.StatusCode);
            }
        }

        public async Task<List<T>> SearchDocumentsAsync<T>(
            string databaseId,
            string collectionId,
            string query,
            SqlParameterCollection parameters = null,
            int pageSize = -1)
        {
            var options = new FeedOptions
            {
                MaxItemCount = pageSize > 0 ? pageSize : -1,
                MaxDegreeOfParallelism = -1,
                PopulateQueryMetrics = false,
                EnableCrossPartitionQuery = true
            };

            var uri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            var querySpec = new SqlQuerySpec(query);
            if (parameters != null)
            {
                querySpec.Parameters = parameters;
            }

            var list = new List<T>();
            var documentQuery = _client.CreateDocumentQuery(uri, querySpec, options).AsDocumentQuery();
            while (documentQuery.HasMoreResults)
            {
                var response = await documentQuery.ExecuteNextAsync<T>();
                if (response != null)
                {
                    list.AddRange(response);
                }
            }
            return list;
        }

        public async Task<bool> DeleteDatabaseIfExistsAsync(string databaseId)
        {
            var databases = _client.CreateDatabaseQuery().AsEnumerable().ToList();
            var isDatabaseExist = databases.Any(z => z.Id == databaseId);
            if (!isDatabaseExist)
            {
                return true;
            }

            var uri = UriFactory.CreateDatabaseUri(databaseId);
            var response = await _client.DeleteDatabaseAsync(uri);
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> DeleteCollectionIfExistsAsync(string databaseId, string collectionId)
        {
            var databases = _client.CreateDatabaseQuery().AsEnumerable().ToList();
            var database = databases.FirstOrDefault(z => z.Id == databaseId);
            if (database == null)
            {
                return true;
            }

            var collections = _client.CreateDocumentCollectionQuery(database.SelfLink).AsEnumerable().ToList();
            var isCollectionExist = collections.Any(z => z.Id == collectionId);
            if (!isCollectionExist)
            {
                return true;
            }

            var uri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            var response = await _client.DeleteDocumentCollectionAsync(uri);
            return IsResponseValid(response.StatusCode);
        }

        public void Dispose()
        {
            _client?.Dispose();
        }

        private static RequestOptions GetRequestOptions(
            string partitionKey = null,
            int? throughput = null,
            int? sharedThroughput = null,
            CosmosConsistencyLevel consistencyLevel = CosmosConsistencyLevel.Strong)
        {
            var requestOptions = new RequestOptions();
            if (throughput != null)
            {
                requestOptions.OfferThroughput = throughput;
            }

            if (sharedThroughput != null)
            {
                requestOptions.SharedOfferThroughput = sharedThroughput;
            }

            if (partitionKey != null)
            {
                requestOptions.PartitionKey = new PartitionKey(partitionKey);
            }

            requestOptions.ConsistencyLevel = GetConsistencyLevel(consistencyLevel);

            return requestOptions;
        }

        private static DocumentCollection GetDocumentCollection(
            string collectionId,
            string partitionKey = null,
            IndexingMode indexingMode = IndexingMode.Consistent)
        {
            var documentCollection = new DocumentCollection();
            documentCollection.Id = collectionId;

            var rangeIndex = new RangeIndex(DataType.String);
            rangeIndex.Precision = -1;
            var indexingPolicy = new IndexingPolicy(rangeIndex);
            indexingPolicy.IndexingMode = indexingMode;
            documentCollection.IndexingPolicy = indexingPolicy;

            if (partitionKey != null)
            {
                var partitionKeyDefinition = new PartitionKeyDefinition();
                partitionKeyDefinition.Paths = new Collection<string> { partitionKey };
                documentCollection.PartitionKey = partitionKeyDefinition;
            }

            return documentCollection;
        }

        private static bool IsResponseValid(HttpStatusCode code)
        {
            var status = (int)code;
            return 200 <= status && status < 300;
        }

        private static ConsistencyLevel GetConsistencyLevel(CosmosConsistencyLevel cosmosConsistencyLevel)
        {
            return cosmosConsistencyLevel switch
            {
                CosmosConsistencyLevel.Strong => ConsistencyLevel.Strong,
                CosmosConsistencyLevel.BoundedStaleness => ConsistencyLevel.BoundedStaleness,
                CosmosConsistencyLevel.Session => ConsistencyLevel.Session,
                CosmosConsistencyLevel.Eventual => ConsistencyLevel.Eventual,
                CosmosConsistencyLevel.ConsistentPrefix => ConsistencyLevel.ConsistentPrefix,
                _ => throw new NotSupportedException($"{nameof(cosmosConsistencyLevel)} value({cosmosConsistencyLevel}) is not supported"),
            };
        }

        private static IndexingMode GetIndexingMode(CosmosIndexingMode cosmosIndexingMode)
        {
            return cosmosIndexingMode switch
            {
                CosmosIndexingMode.Consistent => IndexingMode.Consistent,
                CosmosIndexingMode.Lazy => IndexingMode.Lazy,
                CosmosIndexingMode.None => IndexingMode.None,
                _ => throw new NotSupportedException($"{nameof(cosmosIndexingMode)} value({cosmosIndexingMode}) is not supported"),
            };
        }
    }
}
