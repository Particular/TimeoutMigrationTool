namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenConstants
    {
        public const string ToolStateId = "TimeoutMigrationTool/State";
        public const string ArchivedToolStateIdPrefix = "TimeoutMigrationTool/MigrationRun-";
        public const int DefaultPagingSize = 1024;
        public const string MigrationOngoingPrefix = "__hidden__";
        public const string MigrationDonePrefix = "__migrated__";
        public const string DefaultTimeoutPrefix  = "TimeoutDatas";
        public const string BatchPrefix = "batch";
    }
}