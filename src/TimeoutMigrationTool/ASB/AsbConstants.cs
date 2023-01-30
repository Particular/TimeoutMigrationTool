namespace Particular.TimeoutMigrationTool.ASB
{
    public class AsbConstants
    {
        public const string MigrationQueue = "timeouts-staging";
        public const string NServicebusMigrationDestination = "NServiceBus.Migration.Destination";
        public const string NServicebusMigrationScheduledTime = "NServiceBus.Migration.ScheduledTime";
    }
}