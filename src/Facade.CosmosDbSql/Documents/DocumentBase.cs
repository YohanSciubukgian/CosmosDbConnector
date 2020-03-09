using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Newtonsoft.Json;

namespace Connector.CosmosDbSql.Documents
{
    public class DocumentBase : IEquatable<DocumentBase>
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("key")]
        public string Key { get; set; }

        public DocumentBase()
        {
        }

        public DocumentBase(string id, string key)
        {
            Id = id;
            Key = key;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as DocumentBase);
        }

        public bool Equals([AllowNull] DocumentBase other)
        {
            return other != null &&
                   Id == other.Id;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Id);
        }

        public static bool operator ==(DocumentBase left, DocumentBase right)
        {
            return EqualityComparer<DocumentBase>.Default.Equals(left, right);
        }

        public static bool operator !=(DocumentBase left, DocumentBase right)
        {
            return !(left == right);
        }
    }

    public class DocumentBase<T> : DocumentBase
    {
        public T Document { get; set; }

        public DocumentBase() : base()
        {
        }

        public DocumentBase(string id, string key)
             : base(id, key)
        {
        }

        public DocumentBase(string id, string key, T document)
             : base(id, key)
        {
            Document = document;
        }
    }
}
