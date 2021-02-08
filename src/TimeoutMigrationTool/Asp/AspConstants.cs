namespace Particular.TimeoutMigrationTool.Asp
{
    public static class AspConstants
    {
        public const string MigrationHiddenEndpointNameFormat = "__hidden__{0}__{1}";
        public const string PartitionKeyScope = "yyyyMMddHH";
        public const string MigrationTableName = "timeoutsmigration";
        public const string ToolStateTableName = "timeoutsmigrationtoolstate";
    }
}