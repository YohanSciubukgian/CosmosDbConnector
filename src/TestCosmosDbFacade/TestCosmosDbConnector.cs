using System;
using System.Threading.Tasks;
using Connector.CosmosDbSql.Connectors.AzureCosmosDbV2;
using Connector.CosmosDbSql.Connectors.AzureCosmosDbV3;
using Connector.CosmosDbSql.Documents;
using CosmosDbConnector.Tests.Dtos;
using Xunit;

namespace CosmosDbConnector.Tests
{
    public class TestCosmosDbConnector : IDisposable
    {
        private readonly IAzureCosmosDbV2Connector _azureCosmosDbV2Connector;
        private readonly IAzureCosmosDbV3Connector _azureCosmosDbV3Connector;
        private const string DATABASE_ID_V2 = "test_database_v2";
        private const string DATABASE_ID_V3 = "test_database_v3";
        private const string PARTITION_KEY = "/key";

        public TestCosmosDbConnector()
        {
            var endpoint = "https://localhost:8081/";
            var key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            _azureCosmosDbV2Connector = new AzureCosmosDbV2Connector(endpoint, key);
            _azureCosmosDbV3Connector = new AzureCosmosDbV3Connector(endpoint, key);
        }

        [Fact]
        public async Task SampleTestV2()
        {
            // Arrange
            string collectionId = "test_collection_v2";
            var documentId = "1234";
            var companyName = "company v2";
            var mock = new CompanyMock(companyName);
            var document = new DocumentBase<ICompanyMock>(documentId, PARTITION_KEY, mock);

            // Act
            await _azureCosmosDbV2Connector.CreateDatabaseIfNotExistsAsync(DATABASE_ID_V2, 400);
            await _azureCosmosDbV2Connector.CreateCollectionIfNotExistsAsync(DATABASE_ID_V2, collectionId, PARTITION_KEY);
            await _azureCosmosDbV2Connector.CreateDocumentAsync(DATABASE_ID_V2, collectionId, document);
            var responseDocuments = await _azureCosmosDbV2Connector.SearchDocumentsAsync<DocumentBase<CompanyMock>>(
                DATABASE_ID_V2,
                collectionId,
                "select * from c");

            // Assert
            Assert.NotNull(responseDocuments);
            Assert.Single(responseDocuments);
            Assert.Equal(documentId, responseDocuments[0].Id);
            Assert.Equal(companyName, responseDocuments[0].Document.Name);
        }

        [Fact]
        public async Task SampleTestV3()
        {
            // Arrange
            string collectionId = "test_collection_v3";
            var documentId = "9876";
            var companyName = "company v3";
            var mock = new CompanyMock(companyName);
            var document = new DocumentBase<ICompanyMock>(documentId, PARTITION_KEY, mock);

            // Act
            await _azureCosmosDbV3Connector.CreateDatabaseIfNotExistsAsync(DATABASE_ID_V3, 400);
            await _azureCosmosDbV3Connector.CreateCollectionIfNotExistsAsync(DATABASE_ID_V3, collectionId, PARTITION_KEY);
            await _azureCosmosDbV3Connector.CreateDocumentAsync(DATABASE_ID_V3, collectionId, document);
            var responseDocuments = await _azureCosmosDbV3Connector.SearchDocumentsAsync<DocumentBase<CompanyMock>>(
                DATABASE_ID_V3,
                collectionId,
                "select * from c");

            // Assert
            Assert.NotNull(responseDocuments);
            Assert.Single(responseDocuments);
            Assert.Equal(documentId, responseDocuments[0].Id);
            Assert.Equal(companyName, responseDocuments[0].Document.Name);
        }

        public void Dispose()
        {
            if (_azureCosmosDbV2Connector != null)
            {
                _azureCosmosDbV2Connector.DeleteDatabaseIfExistsAsync(DATABASE_ID_V2).GetAwaiter().GetResult();
                _azureCosmosDbV2Connector.Dispose();
            }

            if (_azureCosmosDbV3Connector != null)
            {
                _azureCosmosDbV3Connector.DeleteDatabaseIfExistsAsync(DATABASE_ID_V3).GetAwaiter().GetResult();
                _azureCosmosDbV3Connector.Dispose();
            }
        }
    }
}
