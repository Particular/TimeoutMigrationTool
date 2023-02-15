namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using RabbitMQ.Client;

    public class RabbitMqTimeoutTarget : ITimeoutsTarget, ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        public RabbitMqTimeoutTarget(ILogger logger, string targetConnectionString, bool useV1)
        {
            this.logger = logger;
            this.targetConnectionString = targetConnectionString;
            this.useV1 = useV1;
            exchange = useV1 ? v1Exchange : v2Exchange;
            batchWriter = new RabbitBatchWriter(logger, targetConnectionString, useV1);
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

            using var connection = factory.CreateConnection();

            var v1ExchangeExists = CheckIfExchangeExists(connection, v1Exchange);
            var v2ExchangeExists = CheckIfExchangeExists(connection, v2Exchange);

            if (useV1 && !v1ExchangeExists) //trying to use v1 delay infrastructure
            {
                if (v2ExchangeExists)
                {
                    result.Problems.Add($"The v1 delay infrastructure does not exist on the RabbitMQ broker, but the v2 delay infrastructure does exist. Remove the '--{ApplicationOptions.UseRabbitDelayInfrastructureVersion1}' option to use the v2 delay infrastructure.");
                }
                else
                {
                    result.Problems.Add("No delay infrastructure found on the RabbitMQ broker. Create the delay infrastructure before running this tool.");
                }

                return new ValueTask<MigrationCheckResult>(result);
            }
            else if (useV1 == false && !v2ExchangeExists) //trying to use v2 delay infrastructure
            {
                if (v1ExchangeExists)
                {
                    result.Problems.Add($"The v2 delay infrastructure does not exist on the RabbitMQ broker, but the v1 delay infrastructure does exist. Add the '--{ApplicationOptions.UseRabbitDelayInfrastructureVersion1}' option to use the v1 delay infrastructure.");
                }
                else
                {
                    result.Problems.Add("No delay infrastructure found on the RabbitMQ broker. Create the delay infrastructure before running this tool.");
                }

                return new ValueTask<MigrationCheckResult>(result);
            }

            foreach (var destination in endpoint.Destinations)
            {
                var destinationExists = CheckIfQueueExists(connection, destination);

                if (destinationExists == false)
                {
                    result.Problems.Add($"There is no queue for destination '{destination}'.");
                    continue;
                }

                using var channel = connection.CreateModel();

                if (CheckIfExchangeExists(connection, destination))
                {
                    channel.ExchangeBind(destination, exchange, $"#.{destination}");
                }
                else
                {
                    channel.QueueBind(destination, exchange, $"#.{destination}");
                }
            }

            return new ValueTask<MigrationCheckResult>(result);
        }

        bool CheckIfExchangeExists(IConnection connection, string exchange)
        {
            using var channel = connection.CreateModel();

            try
            {
                channel.ExchangeDeclarePassive(exchange);
                return true;
            }
            catch
            {
                return false;
            }
        }

        bool CheckIfQueueExists(IConnection connection, string queue)
        {
            using var channel = connection.CreateModel();

            try
            {
                channel.QueueDeclarePassive(queue);
                return true;
            }
            catch
            {
                return false;
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

        readonly ILogger logger;
        readonly string targetConnectionString;
        readonly bool useV1;
        readonly string exchange;
        readonly RabbitBatchWriter batchWriter;
        readonly ConnectionFactory factory;

        const string v1Exchange = "nsb.delay-delivery";
        const string v2Exchange = "nsb.v2.delay-delivery";

        const int maxNumberOfBitsToUse = 28;

        public const int MaxDelayInSeconds = (1 << maxNumberOfBitsToUse) - 1;
        public const int MaxLevel = maxNumberOfBitsToUse - 1;
    }
}