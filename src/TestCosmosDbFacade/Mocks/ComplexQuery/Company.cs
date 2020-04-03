using System.Collections.Generic;

namespace CosmosDbConnector.Tests.Dtos
{
    public class Company
    {
        public string Name { get; set; }
        public IEnumerable<OfficeAddress> OfficeAddresses { get; set; }

        public Company()
        {
        }

        public Company(string name, IEnumerable<OfficeAddress> officeAddresses)
        {
            Name = name;
            OfficeAddresses = officeAddresses;
        }
    }
}
