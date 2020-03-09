namespace Connector.CosmosDbSql.Enums
{
    public enum CosmosConsistencyLevel
    {
        Strong = 0,
        BoundedStaleness = 1,
        Session = 2,
        Eventual = 3,
        ConsistentPrefix = 4
    }
}
