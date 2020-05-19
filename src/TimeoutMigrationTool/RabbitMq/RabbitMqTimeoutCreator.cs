using System;
using System.ComponentModel;
using RabbitMQ.Client;

namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class RabbitMqTimeoutCreator : ICreateTransportTimeouts
    {
        public RabbitMqTimeoutCreator(string targetConnectionString)
        {
            this.targetConnectionString = targetConnectionString;
            this.batchWriter = new RabbitBatchWriter(targetConnectionString);
            this.messagePump = new RabbitStagePump(targetConnectionString);

            Init();
        }

        private Task Init()
        {
            this.factory = new ConnectionFactory();
            factory.Uri = new Uri(this.targetConnectionString);

            return CreateStagingQueue();
        }

        private Task CreateStagingQueue()
        {
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare("TimeoutMigrationTool_Staging", true, false, false, null);
                channel.ExchangeDeclare("TimeoutMigrationTool_Staging", ExchangeType.Fanout, true);
                channel.QueueBind("TimeoutMigrationTool_Staging", "TimeoutMigrationTool_Staging", string.Empty);
            }

            return Task.FromResult(true);

        }

        public Task StageBatch(List<TimeoutData> timeouts)
        {
            return this.batchWriter.WriteTimeoutsToStagingQueue(timeouts, StagingQueueName);
        }

        public Task CompleteBatch(int number)
        {
            return messagePump.CompleteBatch(number);
        }

        private readonly RabbitStagePump messagePump;
        private readonly RabbitBatchWriter batchWriter;
        readonly string targetConnectionString;
        private ConnectionFactory factory;
        public static string StagingQueueName = "TimeoutMigrationTool_Staging";
    }

    internal class RabbitStagePump
    {
        public RabbitStagePump(string targetConnectionString)
        {
            factory = new ConnectionFactory($"{targetConnectionString} - stage message pump");
        }


        public Task CompleteBatch(int number)
        {

        }

        private ConnectionFactory factory;
    }
}