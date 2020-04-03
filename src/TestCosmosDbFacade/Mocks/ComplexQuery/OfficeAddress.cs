namespace CosmosDbConnector.Tests.Dtos
{
    public class OfficeAddress
    {
        public string Address { get; set; }
        public string City { get; set; }
        public string Country { get; set; }

        public OfficeAddress()
        {
        }

        public OfficeAddress(string address, string city, string country)
        {
            Address = address;
            City = city;
            Country = country;
        }
    }
}
