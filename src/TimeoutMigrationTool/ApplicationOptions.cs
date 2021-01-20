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
        public const string SqlSourceConnectionString = "source";
        public const string SqlSourceDialect = "dialect";
        public const string AbortMigration = "abort";
        public const string EndpointFilter = "endpoint";
        public const string AllEndpoints = "allEndpoints";
        public const string NHibernateSourceConnectionString = "nhbSource";
        public const string NHibernateSourceDialect = "nhbDialect";
    }
}