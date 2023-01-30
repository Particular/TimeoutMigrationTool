namespace Particular.TimeoutMigrationTool.ASB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Azure.Messaging.ServiceBus;
    using Microsoft.Extensions.Logging;

    public class AsbEndpointMigrator : ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        readonly IAzureServiceBusEndpoint _azureServiceBusEndpoint;
        string _queueName;
        readonly ILogger logger;

        public AsbEndpointMigrator(IAzureServiceBusEndpoint azureServiceBusEndpoint, string queueName, ILogger logger)
        {
            _azureServiceBusEndpoint = azureServiceBusEndpoint;
            _queueName = queueName;
            this.logger = logger;
            this.logger.LogInformation($"Creating Migration for {queueName}");
        }

        public ValueTask DisposeAsync() => new ValueTask(Task.CompletedTask);

        public async ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            logger.LogInformation($"Staging Migration for {_queueName}");
            var messageChunks = timeouts.Select(s => MapServiceBusMessage(s)).Chunk(100);
            foreach (var messageChunk in messageChunks)
            {
                await _azureServiceBusEndpoint.SendMessages(AsbConstants.MigrationQueue, messageChunk);
            }
            return messageChunks.Sum(s => s.Length);
        }

        public async ValueTask<int> CompleteBatch(int batchNumber)
        {
            logger.LogInformation($"Completing Migration for {_queueName}");

            var counter = 0;
            await _azureServiceBusEndpoint.ProcessMessages(AsbConstants.MigrationQueue, async (receivedMessage) =>
            {
                var scheduledTime = (DateTime)receivedMessage.ApplicationProperties[AsbConstants.NServicebusMigrationScheduledTime];
                var messageToSend = new ServiceBusMessage()
                {
                    MessageId = Guid.NewGuid().ToString(),
                    CorrelationId = receivedMessage.CorrelationId,
                    ContentType = receivedMessage.ContentType,
                    Body = receivedMessage.Body
                };
                foreach (var appProp in receivedMessage.ApplicationProperties)
                {
                    messageToSend.ApplicationProperties.Add(appProp.Key, appProp.Value);
                }

                if (scheduledTime < DateTime.Now)
                {
                    scheduledTime = DateTime.Now.AddDays(1);
                }

                counter++;
                await _azureServiceBusEndpoint.ScheduleMessage(_queueName, scheduledTime, messageToSend);
            });
            return counter;
        }

        ServiceBusMessage MapServiceBusMessage(TimeoutData timeoutData)
        {
            var serviceBusMessage = new ServiceBusMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                CorrelationId = timeoutData.Id,
                ContentType = "application/json",
                Body = new BinaryData(timeoutData.State)
            };

            foreach (var header in timeoutData.Headers)
            {
                serviceBusMessage.ApplicationProperties.Add(header.Key, header.Value);
            }

            serviceBusMessage.ApplicationProperties.Add(AsbConstants.NServicebusMigrationDestination, _queueName);
            serviceBusMessage.ApplicationProperties.Add(AsbConstants.NServicebusMigrationScheduledTime, timeoutData.Time);

            return serviceBusMessage;
        }
    }
}