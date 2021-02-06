namespace Particular.TimeoutMigrationTool.Asp
{
    public static class AspConstants
    {
        public const string MigrationOngoingPrefix = "__hidden__";
        public const string PartitionKeyScope = "yyyyMMddHH";
        public const string MigrationTableName = "timeoutsmigration";
        public const string ToolStateTableName = "timeoutsmigrationtoolstate";
    }
}