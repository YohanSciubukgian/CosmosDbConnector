using System.Collections.Generic;

namespace CosmosDbConnector.Tests.Mocks.ComplexQuery
{
    public class CompanyResult
    {
        public string CompanyName { get; set; }
        public IEnumerable<string> Countries { get; set; }

        public CompanyResult()
        {
        }

        public CompanyResult(string companyName, IEnumerable<string> countries)
        {
            CompanyName = companyName;
            Countries = countries;
        }
    }
}
