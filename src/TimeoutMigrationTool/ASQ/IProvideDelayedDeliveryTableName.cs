namespace Particular.TimeoutMigrationTool.ASQ
{
    public interface IProvideDelayedDeliveryTableName
    {
        string GetDelayedDeliveryTableName(string endpointName);
        string GetStagingTableName(string endpointName);
    }
}
