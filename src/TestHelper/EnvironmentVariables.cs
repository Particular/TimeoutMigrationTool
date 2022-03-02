using System.Collections.Generic;

public static class EnvironmentVariables
{
    public const string AzureStorageConnectionString = "AzureStorageConnectionString";

    public const string Raven4Url = "Raven4Url";

    public const string OracleConnectionString = "OracleConnectionString";

    public const string RabbitMqHost = "RabbitMqHost";

    public const string Raven3Url = "Raven3Url";

    public const string SqlServerConnectionString = "SqlServerConnectionString";

    public static IReadOnlyList<string> Names { get; } = new List<string>
        {
            AzureStorageConnectionString,
            Raven4Url,
            OracleConnectionString,
            RabbitMqHost,
            Raven3Url,
            SqlServerConnectionString,
        };
}