namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using RabbitMQ.Client;

    public static class QueueCreator
    {
        public static string StagingQueueName = "TimeoutMigrationTool_Staging";
        public static string StagingExchangeName = "TimeoutMigrationTool_Staging";

        public static void CreateStagingInfrastructure(IModel model)
        {
            model.QueueDeclare(StagingQueueName, true, false, false, null);
            model.ExchangeDeclare(StagingQueueName, ExchangeType.Fanout, true);
            model.QueueBind(StagingQueueName, StagingExchangeName, string.Empty);
        }

        public static uint GetStagingQueueMessageLength(IModel model)
        {
            return model.QueueDeclarePassive(StagingQueueName).MessageCount;
        }

        public static void PurgeStagingQueue(IModel model)
        {
            model.QueuePurge(StagingQueueName);
        }

        public static void DeleteStagingInfrastructure(IModel model)
        {
            model.QueueUnbind(StagingQueueName, StagingExchangeName, string.Empty);
            model.ExchangeDelete(StagingQueueName, false);
            model.QueueDelete(StagingQueueName, false, false);
        }
    }
}