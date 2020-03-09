namespace CosmosDbConnector.Tests.Dtos
{
    public class CompanyMock : ICompanyMock
    {
        public string Name { get; set; }

        public CompanyMock(string name)
        {
            Name = name;
        }
    }
}
