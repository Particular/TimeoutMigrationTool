namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using RabbitMQ.Client;

    public class RabbitMqTimeoutCreator : ICreateTransportTimeouts
    {
        public RabbitMqTimeoutCreator(ILogger logger, string targetConnectionString)
        {
            this.logger = logger;
            this.targetConnectionString = targetConnectionString;
            batchWriter = new RabbitBatchWriter(logger, targetConnectionString);
        }

        public async Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            factory = new ConnectionFactory();
            factory.Uri = new Uri(targetConnectionString);

            await CreateStagingQueue();

            return await VerifyEndpointIsReadyForNativeTimeouts(endpoint);
        }

        public Task<int> StageBatch(IReadOnlyList<TimeoutData> timeouts)
        {
            logger.LogDebug($"Writing {timeouts.Count} timeout to queue {QueueCreator.StagingQueueName}");

            return batchWriter.WriteTimeoutsToStagingQueue(timeouts, QueueCreator.StagingQueueName);
        }

        public async Task<int> CompleteBatch(int number)
        {
            using (var messagePump = new RabbitStagePump(logger, targetConnectionString, QueueCreator.StagingQueueName))
            {
                return await messagePump.CompleteBatch(number);
            }
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

                    try
                    {
                        channel.ExchangeDeclarePassive("nsb.delay-delivery");
                    }
                    catch (Exception)
                    {
                        result.Problems.Add("The delivery infrastructure on rabbit broker does not exist. It means that the endpoint is running old version of Rabbit Transport package.");
                        return Task.FromResult(result);
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
        readonly RabbitBatchWriter batchWriter;

        const int MaxDelayInSeconds = (1 << MaxLevel) - 1;

        public const int MaxLevel = 28;
    }
}