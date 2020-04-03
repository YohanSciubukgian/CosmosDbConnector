using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Connector.CosmosDbSql.Connectors.AzureCosmosDbV2;
using Connector.CosmosDbSql.Connectors.AzureCosmosDbV3;
using Connector.CosmosDbSql.Documents;
using CosmosDbConnector.Tests.Dtos;
using CosmosDbConnector.Tests.Helpers;
using CosmosDbConnector.Tests.Mocks.ComplexQuery;
using Newtonsoft.Json.Linq;
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
        public async Task SimpleQueryUsingSdkV2()
        {
            // Arrange
            var collectionId = "test_simple_query_v2";
            var documentId = "1234";
            var companies = GetCompanies();
            var document = new DocumentBase<IEnumerable<ICompany>>(documentId, PARTITION_KEY, companies);

            // Act
            await _azureCosmosDbV2Connector.CreateDatabaseIfNotExistsAsync(DATABASE_ID_V2, 400);
            await _azureCosmosDbV2Connector.CreateCollectionIfNotExistsAsync(DATABASE_ID_V2, collectionId, PARTITION_KEY);
            await _azureCosmosDbV2Connector.CreateItemAsync(DATABASE_ID_V2, collectionId, document);
            var responseDocuments = await _azureCosmosDbV2Connector.SearchDocumentsAsync<DocumentBase<IEnumerable<JObject>>>(
                DATABASE_ID_V2,
                collectionId,
                "select * from c");

            // Assert
            Asserts(responseDocuments, documentId);
        }

        [Fact]
        public async Task SimpleQueryUsingSdkV3()
        {
            // Arrange
            var collectionId = "test_simple_query_v3";
            var documentId = "9876";
            var companies = GetCompanies();
            var document = new DocumentBase<IEnumerable<ICompany>>(documentId, PARTITION_KEY, companies);

            // Act
            await _azureCosmosDbV3Connector.CreateDatabaseIfNotExistsAsync(DATABASE_ID_V3, 400);
            await _azureCosmosDbV3Connector.CreateCollectionIfNotExistsAsync(DATABASE_ID_V3, collectionId, PARTITION_KEY);
            await _azureCosmosDbV3Connector.CreateItemAsync(DATABASE_ID_V3, collectionId, document);
            var responseDocuments = await _azureCosmosDbV3Connector.SearchDocumentsAsync<DocumentBase<IEnumerable<JObject>>>(
                DATABASE_ID_V3,
                collectionId,
                "select * from c");

            // Assert
            Asserts(responseDocuments, documentId);
        }

        [Fact]
        public async Task ComplexQueryUsingSdkV3()
        {
            // Arrange
            var collectionIdV3 = "test_complex_query_v3";
            var companies = MockHelper.GetCompanyMocks(1000);
            var query = GetComplexQuery();

            // Act
            await _azureCosmosDbV3Connector.CreateDatabaseIfNotExistsAsync(DATABASE_ID_V3, 10000);
            await _azureCosmosDbV3Connector.CreateCollectionIfNotExistsAsync(DATABASE_ID_V3, collectionIdV3, PARTITION_KEY);
            foreach (var company in companies)
            {
                await _azureCosmosDbV3Connector.CreateItemAsync(DATABASE_ID_V3, collectionIdV3, company);
            }
            var (responseDocumentsV3, requestChargeV3, responseDiagV3) = await _azureCosmosDbV3Connector.SearchDocumentsWithDiagnosticAsync<DocumentBase<CompanyResult>>(
                DATABASE_ID_V3,
                collectionIdV3,
                query);

            // This request should cost between 8 RU & 10 RU
            Assert.InRange(requestChargeV3, 8, 10);
        }

        private static string GetComplexQuery()
        {
            var query = @$"
                SELECT DISTINCT VALUE {{
                    CompanyName : company.{nameof(Company.Name)},
                    Countries : ARRAY(
                        SELECT  VALUE address.{nameof(OfficeAddress.Country)} 
                        FROM    address IN company.{nameof(Company.OfficeAddresses)} 
                    )
                }}
                FROM    c.Document company
                OFFSET 0 LIMIT 20";

            return query;
        }

        private static List<ICompany> GetCompanies()
        {
            return new List<ICompany>
            {
                new CompanyBar("bar-name", "bar-value"),
                new CompanyFoo("foo-name", 42),
            };
        }

        private static void Asserts(List<DocumentBase<IEnumerable<JObject>>> responseDocuments, string documentId)
        {
            Assert.NotNull(responseDocuments);
            Assert.Single(responseDocuments);
            Assert.Equal(documentId, responseDocuments[0].Id);
            Assert.Equal(2, responseDocuments[0].Document.Count());

            Assert.Equal("bar-name", responseDocuments[0].Document.First()[nameof(CompanyBar.Name)]);
            Assert.Equal("bar-value", responseDocuments[0].Document.First()[nameof(CompanyBar.BarValue)]);

            Assert.Equal("foo-name", responseDocuments[0].Document.Last()[nameof(CompanyFoo.Name)]);
            Assert.Equal(42, responseDocuments[0].Document.Last()[nameof(CompanyFoo.FooValue)]);
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
