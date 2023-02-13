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
            v1Exchange = "nsb.delay-delivery";
            v2Exchange = "nsb.v2.delay-delivery";
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
                        channel.ExchangeDeclarePassive(v1Exchange);
                        v1ExchangeExists = true;
                    }
                    catch (Exception)
                    {
                        v1ExchangeExists = false;
                    }

                    try
                    {
                        channel.ExchangeDeclarePassive(v2Exchange);
                        v2ExchangeExists = true;
                    }
                    catch (Exception)
                    {
                        v2ExchangeExists = false;
                    }

                    if (useV1 && !v1ExchangeExists) //trying to use v1 delay infrastructure
                    {
                        if (v2ExchangeExists)
                        {
                            result.Problems.Add($"The v1 delay infrastructure on RabbitMQ broker does not exist. The v2 delay infrastructure on the broker does exist. Try not using the --{ApplicationOptions.UseRabbitDelayInfrastructureVersion1} flag.");
                        }
                        else
                        {
                            result.Problems.Add("The v1 delay infrastructure on RabbitMQ broker does not exist. It means that the endpoint is running old version of RabbitMQ Transport package.");
                        }

                        return new ValueTask<MigrationCheckResult>(result);
                    }

                    if (useV1 == false && !v2ExchangeExists) //trying to use v2 delay infrastructure
                    {
                        if (v1ExchangeExists)
                        {
                            result.Problems.Add($"The v2 delay infrastructure on RabbitMQ broker does not exist, but the v1 delay infrastructure does. If you want to use the v1 delay infrastructure use the --{ApplicationOptions.UseRabbitDelayInfrastructureVersion1} flag. ");
                        }
                        else
                        {
                            result.Problems.Add("The v2 delay infrastructure on RabbitMQ broker does not exist. It means that the endpoint is running old version of RabbitMQ Transport package.");
                        }

                        return new ValueTask<MigrationCheckResult>(result);
                    }

                    if (CheckIfEndpointIsUsingConventionalRoutingTopology(destination))
                    {
                        channel.ExchangeBind(destination, exchange, $"#.{destination}");
                    }
                    else
                    {
                        channel.QueueBind(destination, exchange, $"#.{destination}");
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
        readonly string exchange;
        readonly string v1Exchange;
        readonly string v2Exchange;
        bool v2ExchangeExists;
        bool v1ExchangeExists;
        bool useV1;

        const int maxNumberOfBitsToUse = 28;

        public const int MaxDelayInSeconds = (1 << maxNumberOfBitsToUse) - 1;
        public const int MaxLevel = maxNumberOfBitsToUse - 1;
    }
}