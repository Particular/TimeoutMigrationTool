using RabbitMQ.Client;

namespace Particular.TimeoutMigrationTool.RabbitMq
{
    public class QueueCreator
    {
        public static string StagingQueueName = "TimeoutMigrationTool_Staging";
        public static string StagingExchangeName = "TimeoutMigrationTool_Staging";

        public static void CreateStagingInfrastructure(IModel model)
        {
            model.QueueDeclare(StagingQueueName, true, false, false, null);
            model.ExchangeDeclare(StagingQueueName, ExchangeType.Fanout, true);
            model.QueueBind(StagingQueueName, StagingExchangeName, string.Empty);
        }

        public static uint GetStatingQueueMessageLength(IModel model)
        {
            return model.QueueDeclarePassive(StagingQueueName).MessageCount;
        }

        public static void PurgeStatingQueue(IModel model)
        {
            model.QueuePurge(StagingQueueName);
        }


    }
}