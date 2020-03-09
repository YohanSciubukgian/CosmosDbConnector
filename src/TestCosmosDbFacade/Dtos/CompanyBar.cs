using System.Collections.Generic;

namespace CosmosDbConnector.Tests.Dtos
{
    public class CompanyBar : ICompany
    {
        public string Name { get; set; }
        public string BarValue { get; set; }

        public CompanyBar(string name, string barValue)
        {
            Name = name;
            BarValue = barValue;
        }
    }
}
