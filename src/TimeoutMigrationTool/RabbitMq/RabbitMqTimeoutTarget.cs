namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using RabbitMQ.Client;

    public class RabbitMqTimeoutTarget : ITimeoutsTarget, ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        public RabbitMqTimeoutTarget(ILogger logger, string targetConnectionString)
        {
            this.logger = logger;
            this.targetConnectionString = targetConnectionString;
            batchWriter = new RabbitBatchWriter(logger, targetConnectionString);
            factory = new ConnectionFactory { Uri = new Uri(targetConnectionString) };
        }

        public async ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            return await VerifyEndpointIsReadyForNativeTimeouts(endpoint);
        }

        public ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(string endpointName)
        {
            CreateStagingQueue();
            return new ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator>(this);
        }

        public ValueTask Abort(string endpointName)
        {
            DeleteStagingQueue();
            return new ValueTask();
        }

        public ValueTask Complete(string endpointName)
        {
            EnsureStagingQueueIsEmpty();
            DeleteStagingQueue();
            return new ValueTask();
        }

        void EnsureStagingQueueIsEmpty()
        {
            using var connection = factory.CreateConnection();
            using var model = connection.CreateModel();
            var stagingQueueLength = QueueCreator.GetStagingQueueMessageLength(model);
            if (stagingQueueLength > 0)
            {
                throw new Exception(
                    $"Unable to complete migration as there are still messages available in the staging queue. Found {stagingQueueLength} messages.");
            }
        }

        public async ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            logger.LogDebug($"Writing {timeouts.Count} timeout to queue {QueueCreator.StagingQueueName}");

            return await batchWriter.WriteTimeoutsToStagingQueue(timeouts, QueueCreator.StagingQueueName);
        }

        public async ValueTask<int> CompleteBatch(int batchNumber)
        {
            using var messagePump = new RabbitStagePump(logger, targetConnectionString, QueueCreator.StagingQueueName);
            return await messagePump.CompleteBatch(batchNumber);
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask();
        }

        ValueTask<MigrationCheckResult> VerifyEndpointIsReadyForNativeTimeouts(EndpointInfo endpoint)
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
                        return new ValueTask<MigrationCheckResult>(result);
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

            return new ValueTask<MigrationCheckResult>(result);
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

        void CreateStagingQueue()
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            QueueCreator.CreateStagingInfrastructure(channel);
        }

        void DeleteStagingQueue()
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            QueueCreator.DeleteStagingInfrastructure(channel);
        }

        string targetConnectionString;
        ConnectionFactory factory;

        readonly ILogger logger;
        readonly RabbitBatchWriter batchWriter;

        const int MaxDelayInSeconds = (1 << MaxLevel) - 1;

        public const int MaxLevel = 28;
    }
}