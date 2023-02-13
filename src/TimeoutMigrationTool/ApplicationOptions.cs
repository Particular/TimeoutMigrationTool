namespace Particular.TimeoutMigrationTool
{
    public class ApplicationOptions
    {
        public const string RavenDatabaseName = "databaseName";
        public const string RavenServerUrl = "serverUrl";
        public const string RavenVersion = "ravenVersion";
        public const string ForceUseIndex = "forceUseIndex";
        public const string RavenTimeoutPrefix = "prefix";
        public const string CutoffTime = "cutoffTime";
        public const string RabbitMqTargetConnectionString = "target";
        public const string MsmqSqlTargetConnectionString = "target";
        public const string SqlTTargetConnectionString = "target";
        public const string SqlTTargetSchema = "schema";
        public const string MsmqSqlTargetSchema = "schema";
        public const string SqlSourceConnectionString = "source";
        public const string SqlSourceDialect = "dialect";
        public const string AspSourceConnectionString = "source";
        public const string AspSourceContainerName = "containerName";
        public const string AspSourcePartitionKeyScope = "partitionKeyScope";
        public const string AspTimeoutTableName = "timeoutTableName";
        public const string AbortMigration = "abort";
        public const string EndpointFilter = "endpoint";
        public const string AllEndpoints = "allEndpoints";
        public const string NHibernateSourceConnectionString = "source";
        public const string NHibernateSourceDialect = "dialect";
        public const string AsqTargetConnectionString = "target";
        public const string AsqDelayedDeliveryTableName = "delayedtablename";
        public const string UseRabbitDelayInfrastructureVersion1 = "useV1";
    }
}