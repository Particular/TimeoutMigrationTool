namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System;
    using RabbitMQ.Client;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    public class RabbitMqTimeoutCreator : ICreateTransportTimeouts
    {
        public RabbitMqTimeoutCreator(ILogger logger, string targetConnectionString)
        {
            this.logger = logger;
            this.targetConnectionString = targetConnectionString;
            batchWriter = new RabbitBatchWriter(logger, targetConnectionString);
            messagePump = new RabbitStagePump(logger, targetConnectionString, QueueCreator.StagingQueueName);
        }

        public async Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            factory = new ConnectionFactory();
            factory.Uri = new Uri(targetConnectionString);

            await CreateStagingQueue();

            return await VerifyEndpointIsReadyForNativeTimeouts(endpoint);
        }

        public Task<int> StageBatch(List<TimeoutData> timeouts)
        {
            logger.LogDebug($"Writing {timeouts.Count} timeout to queue {QueueCreator.StagingQueueName}");

            return batchWriter.WriteTimeoutsToStagingQueue(timeouts, QueueCreator.StagingQueueName);
        }

        public Task<int> CompleteBatch(int number)
        {
            return messagePump.CompleteBatch(number);
        }

        Task<MigrationCheckResult> VerifyEndpointIsReadyForNativeTimeouts(EndpointInfo endpoint)
        {
            var result = new MigrationCheckResult();
            if ((endpoint.LongestTimeout - DateTime.UtcNow).TotalSeconds > MaxDelayInSeconds)
            {
                result.Problems.Add($"{endpoint.EndpointName} - has a timeout that has further away date than allowed {MaxDelayInSeconds} seconds (8.5 years).");
            }

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                foreach (var destination in endpoint.Destinations)
                {
                    try
                    {
                        channel.QueueDeclarePassive(destination);
                    }
                    catch (Exception)
                    {
                        result.Problems.Add($"There is no queue for destination {destination}.");
                        continue;
                    }

                    if (CheckIfEndpointIsUsingConventionalRoutingTopology(destination))
                    {
                        channel.ExchangeBind(destination, "nsb.delay-delivery", $"#.{destination}");
                    }
                    else
                    {
                        channel.QueueBind(destination, "nsb.delay-delivery", $"#.{destination}");
                    }

                }
            }

            return Task.FromResult(result);
        }

        bool CheckIfEndpointIsUsingConventionalRoutingTopology(string destination)
        {
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                try
                {
                    channel.ExchangeDeclarePassive(destination);
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }

        }

        Task CreateStagingQueue()
        {
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                QueueCreator.CreateStagingInfrastructure(channel);
            }

            return Task.CompletedTask;
        }

        string targetConnectionString;
        ConnectionFactory factory;

        readonly ILogger logger;
        readonly RabbitStagePump messagePump;
        readonly RabbitBatchWriter batchWriter;

        const int MaxDelayInSeconds = (1 << MaxLevel) - 1;

        public const int MaxLevel = 28;
    }
}