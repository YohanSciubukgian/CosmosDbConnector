using System.Collections.Generic;
using Newtonsoft.Json;

namespace Connector.CosmosDbSql.Documents
{
    internal class QueryStreamResponse<T>
    {
        [JsonProperty("_rid")]
        public string RequestId { get; set; }

        [JsonProperty("Documents")]
        public List<T> Documents { get; set; }

        [JsonProperty("_count")]
        public int Count { get; set; }
    }
}
