using System.Collections.Generic;

public static class EnvironmentVariables
{
    public const string AzureStorage_ConnectionString = "AzureStorage_ConnectionString";

    public const string CommaSeparatedRavenClusterUrls = "CommaSeparatedRavenClusterUrls";

    public const string OracleConnectionString = "OracleConnectionString";

    public const string RabbitMQ_uri = "RabbitMQ_uri";

    public const string Raven35Url = "Raven35Url";

    public const string SQLServerConnectionString = "SQLServerConnectionString";

    public static IReadOnlyList<string> Names { get; } = new List<string>
        {
            AzureStorage_ConnectionString,
            CommaSeparatedRavenClusterUrls,
            OracleConnectionString,
            RabbitMQ_uri,
            Raven35Url,
            SQLServerConnectionString,
        };
}