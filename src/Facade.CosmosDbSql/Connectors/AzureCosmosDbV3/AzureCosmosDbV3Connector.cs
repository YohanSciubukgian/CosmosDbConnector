using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Connector.CosmosDbSql.Documents;
using Connector.CosmosDbSql.Enums;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Connector.CosmosDbSql.Connectors.AzureCosmosDbV3
{
    public class AzureCosmosDbV3Connector : IAzureCosmosDbV3Connector, IDisposable
    {
        private readonly CosmosClient _client;
        private readonly JsonSerializerSettings _jsonSerializerSettings;
        private readonly Dictionary<(string, string), Container> _containerByDatabaseIdAndContainerId;
        public AzureCosmosDbV3Connector(string endpoint, string key)
        {
            _containerByDatabaseIdAndContainerId = new Dictionary<(string, string), Container>();
            _jsonSerializerSettings = new JsonSerializerSettings()
            {
                DateParseHandling = DateParseHandling.DateTimeOffset,
                NullValueHandling = NullValueHandling.Include
            };

            _client = new CosmosClient(endpoint, key);
        }

        public async Task<bool> CreateDatabaseIfNotExistsAsync(
            string databaseId,
            int? sharedThroughput,
            CosmosConsistencyLevel cosmosConsistencyLevel = CosmosConsistencyLevel.Strong)
        {
            var response = await _client.CreateDatabaseIfNotExistsAsync(
                databaseId,
                sharedThroughput,
                requestOptions: null,
                default(CancellationToken));
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> CreateCollectionIfNotExistsAsync(
            string databaseId,
            string collectionId,
            string partitionKey,
            CosmosConsistencyLevel cosmosConsistencyLevel = CosmosConsistencyLevel.Strong,
            CosmosIndexingMode cosmosIndexingMode = CosmosIndexingMode.Consistent)
        {
            var database = _client.GetDatabase(databaseId);
            var indexingMode = GetIndexingMode(cosmosIndexingMode);
            var properties = new ContainerProperties(collectionId, partitionKey)
            {
                //PartitionKeyDefinitionVersion = PartitionKeyDefinitionVersion.V2,
                PartitionKeyPath = partitionKey,
                IndexingPolicy = new IndexingPolicy
                {
                    Automatic = true,
                    IndexingMode = indexingMode
                }
            };
            var requestOptions = new ItemRequestOptions
            {
                ConsistencyLevel = GetConsistencyLevel(cosmosConsistencyLevel)
            };

            var response = await database.CreateContainerIfNotExistsAsync(
                properties,
                requestOptions: requestOptions,
                cancellationToken: default(CancellationToken));
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> CreateItemAsync<T>(string databaseId, string collectionId, DocumentBase<T> item)
        {
            var container = GetContainer(databaseId, collectionId);
            var response = await container.CreateItemAsync<dynamic>(item);
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> UpdateItemAsync<T>(string databaseId, string collectionId, DocumentBase<T> item)
        {
            var container = GetContainer(databaseId, collectionId);
            var response = await container.ReplaceItemAsync<dynamic>(item, item.Id);
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> UpsertItemAsync<T>(string databaseId, string collectionId, DocumentBase<T> item)
        {
            var container = GetContainer(databaseId, collectionId);
            var response = await container.UpsertItemAsync<dynamic>(item);
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> DeleteItemAsync<T>(string databaseId, string collectionId, string id, string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

            var container = GetContainer(databaseId, collectionId);
            var response = await container.DeleteItemAsync<T>(id, new PartitionKey(partitionKey), null, default(CancellationToken));
            return IsResponseValid(response.StatusCode);
        }

        public async Task<List<T>> SearchDocumentsAsync<T>(
            string databaseId,
            string collectionId,
            string query,
            Dictionary<string, object> parameters = null)
        {
            var queryRequestOptions = GetQueryRequestOptions();
            var container = GetContainer(databaseId, collectionId);
            var queryDefinition = GetQueryDefinition(query, parameters);
            var feedIterator = container.GetItemQueryStreamIterator(queryDefinition, null, queryRequestOptions);

            var list = new List<T>();
            while (feedIterator.HasMoreResults)
            {
                using (var response = await feedIterator.ReadNextAsync())
                {
                    if (response == null)
                    {
                        continue;
                    }

                    var stream = response.Content;
                    using (var reader = new StreamReader(stream))
                    {
                        var contentString = await reader.ReadToEndAsync();
                        var items = JsonConvert.DeserializeObject<QueryStreamResponse<T>>(contentString, _jsonSerializerSettings);
                        if (items?.Documents != null)
                        {
                            list.AddRange(items.Documents);
                        }
                    }
                }
            }

            return list;
        }

        public async Task<bool> DeleteDatabaseIfExistsAsync(string databaseId)
        {
            var isDatabaseExist = await IsDatabaseExist(databaseId);
            if (!isDatabaseExist)
            {
                return true;
            }

            var database = _client.GetDatabase(databaseId);
            var response = await database.DeleteAsync();
            return IsResponseValid(response.StatusCode);
        }

        public async Task<bool> DeleteCollectionIfExistsAsync(string databaseId, string collectionId)
        {
            var isCollectionExist = await IsCollectionExist(databaseId, collectionId);
            if (!isCollectionExist)
            {
                return true;
            }
            
            var container = GetContainer(databaseId, collectionId);
            var response = await container.DeleteContainerAsync();
            return IsResponseValid(response.StatusCode);
        }

        public void Dispose()
        {
            _client?.Dispose();
        }

        private Container GetContainer(string databaseId, string containerId)
        {
            if (!_containerByDatabaseIdAndContainerId.TryGetValue((databaseId, containerId), out var container))
            {
                container = _client.GetContainer(databaseId, containerId);
                _containerByDatabaseIdAndContainerId.Add((databaseId, containerId), container);
            }

            return container;
        }

        private async Task<bool> IsDatabaseExist(string databaseId)
        {
            var iterator = _client.GetDatabaseQueryIterator<DatabaseProperties>();
            while (iterator.HasMoreResults)
            {
                foreach (var databaseProperties in await iterator.ReadNextAsync())
                {
                    if (databaseProperties.Id == databaseId)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private async Task<bool> IsCollectionExist(string databaseId, string collectionId)
        {
            var isDatabaseExist = await IsDatabaseExist(databaseId);
            if (!isDatabaseExist)
            {
                return false;
            }

            var database = _client.GetDatabase(databaseId);
            var iterator = database.GetContainerQueryIterator<ContainerProperties>();
            while (iterator.HasMoreResults)
            {
                foreach (var containerProperties in await iterator.ReadNextAsync())
                {
                    if (containerProperties.Id == collectionId)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private static bool IsResponseValid(HttpStatusCode code)
        {
            var status = (int)code;
            return 200 <= status && status < 300;
        }

        private static QueryRequestOptions GetQueryRequestOptions()
        {
            var options = new QueryRequestOptions
            {
                MaxItemCount = -1,
                MaxConcurrency = -1,
                MaxBufferedItemCount = -1
            };
            return options;
        }

        private static QueryDefinition GetQueryDefinition(
            string query,
            Dictionary<string, object> parameters)
        {
            var queryDefinition = new QueryDefinition(query);
            if (parameters != null)
            {
                foreach (var (key, value) in parameters)
                {
                    queryDefinition.WithParameter(key, value);
                }
            }

            return queryDefinition;
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