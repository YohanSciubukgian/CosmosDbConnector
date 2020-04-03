using Microsoft.Azure.Cosmos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Connector.CosmosDbSql.Connectors.AzureCosmosDbV3
{
    public interface IAzureCosmosDbV3Connector : IAzureCosmosDbConnector
    {
        Task<List<T>> SearchDocumentsAsync<T>(
            string databaseId,
            string collectionId,
            string query,
            Dictionary<string, object> parameters = null);

        Task<(List<T> documents, double requestCharge, List<string> responseDiagnostics)> SearchDocumentsWithDiagnosticAsync<T>(
            string databaseId,
            string collectionId,
            string query,
            Dictionary<string, object> parameters = null);
    }
}