using System.Threading.Tasks;
using Connector.CosmosDbSql.Documents;
using Connector.CosmosDbSql.Enums;

namespace Connector.CosmosDbSql.Connectors
{
    public interface IAzureCosmosDbConnector
    {
        Task<bool> CreateDatabaseIfNotExistsAsync(
            string databaseId,
            int? sharedThroughput,
            CosmosConsistencyLevel cosmosConsistencyLevel = CosmosConsistencyLevel.Strong);
        Task<bool> CreateCollectionIfNotExistsAsync(
            string databaseId,
            string collectionId,
            string partitionKey,
            CosmosConsistencyLevel consistencyLevel = CosmosConsistencyLevel.Strong,
            CosmosIndexingMode indexingMode = CosmosIndexingMode.Consistent);

        Task<bool> CreateDocumentAsync<T>(string databaseId, string collectionId, DocumentBase<T> item);
        Task<bool> UpdateDocumentAsync<T>(string databaseId, string collectionId, DocumentBase<T> item);
        Task<bool> UpsertDocumentAsync<T>(string databaseId, string collectionId, DocumentBase<T> item);
        Task<bool> DeleteItemAsync<T>(string databaseId, string collectionId, string id, string partitionKey = null);

        Task<bool> DeleteDatabaseIfExistsAsync(string databaseId);
        Task<bool> DeleteCollectionIfExistsAsync(string databaseId, string collectionId);
        void Dispose();
    }
}
