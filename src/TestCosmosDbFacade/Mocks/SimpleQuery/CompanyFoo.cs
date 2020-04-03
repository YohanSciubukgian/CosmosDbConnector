namespace CosmosDbConnector.Tests.Dtos
{
    public class CompanyFoo : ICompany
    {
        public string Name { get; set; }
        public decimal FooValue { get; set; }

        public CompanyFoo(string name, decimal fooValue)
        {
            Name = name;
            FooValue = fooValue;
        }
    }
}
