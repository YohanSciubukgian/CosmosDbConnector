using Connector.CosmosDbSql.Documents;
using CosmosDbConnector.Tests.Dtos;
using System.Collections.Generic;

namespace CosmosDbConnector.Tests.Helpers
{
    public static class MockHelper
    {
        public static IEnumerable<DocumentBase<Company>> GetCompanyMocks(int numberOfMocks)
        {
            var companies = new List<DocumentBase<Company>>();
            for (var i = 0; i < numberOfMocks; i++)
            {
                companies.Add(GetCompanyMock(i));
            }

            return companies;
        }

        private static DocumentBase<Company> GetCompanyMock(int i)
        {
            var officeAddresses = new List<OfficeAddress>();
            for (var j = 0; j < 10; j++)
            {
                officeAddresses.Add(new OfficeAddress($"office_{i}.address_{j}", $"office_{i}.city_{j}", $"office_{i}.country_{j}"));
            }
            var company = new Company($"company_name_{i}", officeAddresses);
            return new DocumentBase<Company>(company.Name, "/key", company);
        }
    }
}
