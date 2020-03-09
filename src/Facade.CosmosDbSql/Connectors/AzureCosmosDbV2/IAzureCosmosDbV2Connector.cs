using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;

namespace Connector.CosmosDbSql.Connectors.AzureCosmosDbV2
{
    public interface IAzureCosmosDbV2Connector : IAzureCosmosDbConnector
    {
        Task<List<T>> SearchDocumentsAsync<T>(
            string databaseId,
            string collectionId,
            string query,
            SqlParameterCollection parameters = null,
            int pageSize = -1);
    }
}
