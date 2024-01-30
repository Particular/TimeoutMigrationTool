namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;
    using RabbitMQ.Client.Exceptions;

    public class RabbitStagePump : IDisposable
    {
        public RabbitStagePump(ILogger logger, string targetConnectionString, string queueName)
        {
            factory = new ConnectionFactory
            {
                Uri = new Uri(targetConnectionString),
                DispatchConsumersAsync = true,
                ConsumerDispatchConcurrency = MaxConcurrency
            };
            prefetchMultiplier = 10;
            this.logger = logger;
            this.queueName = queueName;
        }

        int processedMessages;
        uint messageCount;
        readonly int prefetchMultiplier;
        long numberOfMessagesBeingProcessed;

        // Start
        const int MaxConcurrency = 10;
        CancellationTokenSource messageProcessing;
        IConnection connection;
        AsyncEventingBasicConsumer consumer;
        IModel channel;

        // Stop
        TaskCompletionSource<bool> connectionShutdownCompleted;

        public void Start(int batchNumber)
        {
            messageProcessing = new CancellationTokenSource();

            connection = factory.CreateConnection("TimoutMigration - CompleteBatch");

            channel = connection.CreateModel();

            channel.ConfirmSelect();
            messageCount = QueueCreator.GetStagingQueueMessageLength(channel);

            logger.LogDebug($"Pushing {messageCount} to the native timeout structure");
            var prefetchCount = (long)MaxConcurrency * prefetchMultiplier;

            channel.BasicQos(0, (ushort)Math.Min(prefetchCount, ushort.MaxValue), false);

            consumer = new AsyncEventingBasicConsumer(channel);

            connection.ConnectionShutdown += Connection_ConnectionShutdown;

            consumer.Received += Consumer_Received;

            channel.BasicConsume(queueName, false, $"Batch-{batchNumber}", consumer);
        }

        public async Task Stop()
        {
            consumer.Received -= Consumer_Received;
            messageProcessing.Cancel();

            while (Interlocked.Read(ref numberOfMessagesBeingProcessed) > 0)
            {
                await Task.Delay(50);
            }

            connectionShutdownCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (connection.IsOpen)
            {
                connection.Close();
            }
            else
            {
                connectionShutdownCompleted.SetResult(true);
            }

            await connectionShutdownCompleted.Task;
        }

        void Connection_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            if (e.Initiator == ShutdownInitiator.Application && e.ReplyCode == 200)
            {
                connectionShutdownCompleted?.TrySetResult(true);
            }
        }

        async Task Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            if (messageProcessing.Token.IsCancellationRequested)
            {
                return;
            }

            try
            {
                Interlocked.Increment(ref numberOfMessagesBeingProcessed);

                await Process(eventArgs);
            }
            catch (Exception ex)
            {
                logger.LogError($"Failed to process message. Returning message to queue... {ex}");
                await consumer.Model.BasicRejectAndRequeueIfOpen(eventArgs.DeliveryTag);
                messageProcessing.Cancel();
            }
            finally
            {
                Interlocked.Decrement(ref numberOfMessagesBeingProcessed);
            }
        }

        async Task Process(BasicDeliverEventArgs message)
        {
            var delayExchangeName = Encoding.UTF8.GetString(message.BasicProperties.Headers["TimeoutMigrationTool.DelayExchange"] as byte[]);
            var routingKey = Encoding.UTF8.GetString(message.BasicProperties.Headers["TimeoutMigrationTool.RoutingKey"] as byte[]);

            message.BasicProperties.Headers.Remove("TimeoutMigrationTool.DelayExchange");
            message.BasicProperties.Headers.Remove("TimeoutMigrationTool.RoutingKey");

            using var tokenSource = new CancellationTokenSource();
            consumer.Model.BasicPublish(delayExchangeName, routingKey, true, message.BasicProperties, message.Body);

            if (tokenSource.IsCancellationRequested)
            {
                await consumer.Model.BasicRejectAndRequeueIfOpen(message.DeliveryTag);
            }
            else
            {
                try
                {
                    await consumer.Model.BasicAckSingle(message.DeliveryTag);
                    Interlocked.Increment(ref processedMessages);
                }
                catch (AlreadyClosedException ex)
                {
                    logger.LogWarning($"Failed to acknowledge message because the channel was closed. The message was returned to the queue. {ex}");
                }
            }
        }

        public uint GetMessageCount()
        {
            var data = consumer.Model.QueueDeclarePassive("TimeoutMigrationTool_Staging");
            return data.MessageCount;
        }

        public void Dispose()
        {
            messageProcessing?.Dispose();
            channel?.Dispose();
            connection?.Dispose();
        }

        public async Task<int> CompleteBatch(int number)
        {
            Interlocked.Exchange(ref processedMessages, 0);

            Start(number);
            do
            {
                await Task.Delay(100);
            }
            while (processedMessages < messageCount && !messageProcessing.IsCancellationRequested);

            if (messageProcessing.IsCancellationRequested)
            {
                logger.LogError("The migration was cancelled due to error when completing the batch.");
            }

            if (!messageProcessing.IsCancellationRequested && QueueCreator.GetStagingQueueMessageLength(consumer.Model) > 0)
            {
                throw new InvalidOperationException("Staging queue is not empty after finishing CompleteBatch");
            }

            channel.WaitForConfirms(TimeSpan.FromSeconds(10));

            await Stop();

            return processedMessages;
        }

        ConnectionFactory factory;
        string queueName;

        readonly ILogger logger;
    }
}