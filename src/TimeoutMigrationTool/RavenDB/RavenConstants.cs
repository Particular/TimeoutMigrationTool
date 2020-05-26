namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenConstants
    {
        public const string ToolStateId = "TimeoutMigrationTool/State";
        public const int DefaultPagingSize = 1024;
        public const string MigrationPrefix = "__hidden_";
        public const string DefaultTimeoutPrefix  = "TimeoutDatas";
        public const string BatchPrefix = "batch";
    }
}