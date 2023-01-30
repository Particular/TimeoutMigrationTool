namespace Particular.TimeoutMigrationTool.ASB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Azure.Messaging.ServiceBus;
    using Azure.Messaging.ServiceBus.Administration;
    using Microsoft.Extensions.Logging;

    public interface IAzureServiceBusEndpoint
    {
        Task<QueueProperties> GetQueueAsync(string queueName);

        Task<bool> QueueExistsAsync(string queueName);

        Task<QueueProperties> CreateQueueAsync(string queueName);

        Task SendMessage(string queue, ServiceBusMessage message);

        Task SendMessages(string queue, IEnumerable<ServiceBusMessage> messages);

        Task<long> ScheduleMessage(string queue, DateTime datetime, ServiceBusMessage message);
        Task ProcessMessages(string queue, Func<ServiceBusReceivedMessage, Task> processMessage, int batchCount = 50);

    }

    public class AzureServiceBusEndpoint : IAzureServiceBusEndpoint
    {
        readonly string _connectionString;
        readonly ILogger _logger;

        public AzureServiceBusEndpoint(string connectionString, ILogger logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        public async Task<QueueProperties> GetQueueAsync(string queueName)
        {
            var client = new ServiceBusAdministrationClient(_connectionString);
            var result = await client.GetQueueAsync(queueName);
            return result.Value;
        }

        public async Task<bool> QueueExistsAsync(string queueName)
        {
            var client = new ServiceBusAdministrationClient(_connectionString);
            var result = await client.QueueExistsAsync(queueName);
            return result.Value;
        }

        public async Task<QueueProperties> CreateQueueAsync(string queueName)
        {
            _logger.LogInformation($"Creating queue {queueName}");
            var client = new ServiceBusAdministrationClient(_connectionString);
            var options = new CreateQueueOptions(queueName)
            {
                LockDuration = TimeSpan.FromSeconds(60)
            };
            var result = await client.CreateQueueAsync(options);
            return result.Value;
        }

        public async Task SendMessage(string queue, ServiceBusMessage message)
        {
            var client = GetOrCreateServiceBusClient();
            await client.CreateSender(queue).SendMessageAsync(message);
        }

        public async Task SendMessages(string queue, IEnumerable<ServiceBusMessage> messages)
        {
            _logger.LogInformation($"{DateTime.UtcNow}: Sending {messages.Count()} to queue {queue}");
            var client = GetOrCreateServiceBusClient();
            await client.CreateSender(queue).SendMessagesAsync(messages);
        }

        public async Task<long> ScheduleMessage(string queue, DateTime datetime, ServiceBusMessage message)
        {
            return await GetOrCreateSender(queue)
                .ScheduleMessageAsync(message, datetime);
        }

        public async Task ProcessMessages(string queue, Func<ServiceBusReceivedMessage, Task> processMessage, int batchCount = 50)
        {
            var client = GetOrCreateServiceBusClient();
            var options = new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock };
            var receiver = client.CreateReceiver(queue, options);
            bool hasMessages = true;
            while (hasMessages)
            {
                var messages = await receiver.ReceiveMessagesAsync(batchCount, TimeSpan.FromSeconds(30));
                hasMessages = messages.Count > 0;
                _logger.LogInformation($"{DateTime.UtcNow}: Received {messages.Count} messages from batch, now processing them . . . ");
                foreach (var receivedMessage in messages)
                {
                    await processMessage(receivedMessage);
                    //  _logger.LogInformation($"{DateTime.UtcNow}: Processed a message from the batch");
                    await receiver.CompleteMessageAsync(receivedMessage);
                    //  _logger.LogInformation($"{DateTime.UtcNow}: Completed a message from the batch");
                }
            }
        }

        ServiceBusClient _servicebusClient;
        ServiceBusClient GetOrCreateServiceBusClient()
        {
            _servicebusClient ??= new ServiceBusClient(_connectionString);

            return _servicebusClient;
        }

        Dictionary<string, ServiceBusSender> _senders = new Dictionary<string, ServiceBusSender>();
        ServiceBusSender GetOrCreateSender(string queue)
        {
            if (!_senders.ContainsKey(queue))
            {
                _senders[queue] = GetOrCreateServiceBusClient().CreateSender(queue);
            }
            return _senders[queue];
        }
    }
}