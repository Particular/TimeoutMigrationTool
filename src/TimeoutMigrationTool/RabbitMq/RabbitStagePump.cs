using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Particular.TimeoutMigrationTool.RabbitMq
{
    public class RabbitStagePump : IDisposable
    {
        public RabbitStagePump(ILogger logger, string targetConnectionString, string queueName)
        {
            factory = new ConnectionFactory();
            factory.Uri = new Uri(targetConnectionString);
            this.prefetchMultiplier = 10;
            this.logger = logger;
            this.queueName = queueName;

            exclusiveScheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        }

        static int processedMessages = 0;
        uint messageCount;
        readonly int prefetchMultiplier;

        TaskScheduler exclusiveScheduler;

        // Start
        int maxConcurrency;
        SemaphoreSlim semaphore;
        CancellationTokenSource messageProcessing;
        IConnection connection;
        EventingBasicConsumer consumer;

        // Stop
        TaskCompletionSource<bool> connectionShutdownCompleted;

        public void Start(int batchNumber)
        {
            maxConcurrency = 10;
            messageProcessing = new CancellationTokenSource();
            semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);

            connection = factory.CreateConnection("TimoutMigration - CompleteBatch");

            var channel = connection.CreateModel();

            messageCount = QueueCreator.GetStatingQueueMessageLength(channel);

            logger.LogDebug($"Pushing {messageCount} to the native timeout structure");
            long prefetchCount = (long)maxConcurrency * prefetchMultiplier;

            channel.BasicQos(0, (ushort)Math.Min(prefetchCount, ushort.MaxValue), false);

            consumer = new EventingBasicConsumer(channel);

            connection.ConnectionShutdown += Connection_ConnectionShutdown;

            consumer.Received += Consumer_Received;

            channel.BasicConsume(queueName, false, $"Batch-{batchNumber}", consumer);
        }

        public async Task Stop()
        {
            consumer.Received -= Consumer_Received;
            messageProcessing.Cancel();

            while (semaphore.CurrentCount != maxConcurrency)
            {
                await Task.Delay(50);
            }

            connectionShutdownCompleted = new TaskCompletionSource<bool>();

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

        async void Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var eventRaisingThreadId = Thread.CurrentThread.ManagedThreadId;

            try
            {
                await semaphore.WaitAsync(messageProcessing.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                // The current thread will be the event-raising thread if either:
                //
                // a) the semaphore was entered synchronously (did not have to wait).
                // b) the event was raised on a thread pool thread,
                //    and the semaphore was entered asynchronously (had to wait),
                //    and the continuation happened to be scheduled back onto the same thread.
                if (Thread.CurrentThread.ManagedThreadId == eventRaisingThreadId)
                {
                    // In RabbitMQ.Client 4.1.0, the event is raised by reusing a single, explicitly created thread,
                    // so we are in scenario (a) described above.
                    // We must yield to allow the thread to raise more events while we handle this one,
                    // otherwise we will never process messages concurrently.
                    //
                    // If a future version of RabbitMQ.Client changes its threading model, then either:
                    //
                    // 1) we are in scenario (a), but we *may not* need to yield.
                    //    E.g. the client may raise the event on a new, explicitly created thread each time.
                    // 2) we cannot tell whether we are in scenario (a) or scenario (b).
                    //    E.g. the client may raise the event on a thread pool thread.
                    //
                    // In both cases, we cannot tell whether we need to yield or not, so we must yield.
                    await Task.Yield();
                }

                await Process(eventArgs);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Failed to process message. Returning message to queue... {ex}");
                await consumer.Model.BasicRejectAndRequeueIfOpen(eventArgs.DeliveryTag, exclusiveScheduler);
            }
            finally
            {
                semaphore.Release();
            }
        }

        async Task Process(BasicDeliverEventArgs message)
        {
            string delayExchangeName, routingKey;

            try
            {
                delayExchangeName = Encoding.UTF8.GetString(message.BasicProperties.Headers["TimeoutMigrationTool.DelayExchange"] as byte[]);
                routingKey = Encoding.UTF8.GetString(message.BasicProperties.Headers["TimeoutMigrationTool.RoutingKey"] as byte[]);
                message.BasicProperties.Headers.Remove("TimeoutMigrationTool.DelayExchange");
                message.BasicProperties.Headers.Remove("TimeoutMigrationTool.RoutingKey");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Failed to retrieve delayed exchange name or routing key from the message... {ex}");

                return;
            }

            using (var tokenSource = new CancellationTokenSource())
            {
                consumer.Model.BasicPublish(delayExchangeName, routingKey, true, message.BasicProperties, message.Body);

                if (tokenSource.IsCancellationRequested)
                {
                    await consumer.Model.BasicRejectAndRequeueIfOpen(message.DeliveryTag, exclusiveScheduler);
                }
                else
                {
                    try
                    {
                        await consumer.Model.BasicAckSingle(message.DeliveryTag, exclusiveScheduler);
                        Interlocked.Increment(ref processedMessages);
                    }
                    catch (AlreadyClosedException ex)
                    {
                        Console.Error.WriteLine($"Failed to acknowledge message because the channel was closed. The message was returned to the queue. {ex}");
                    }
                }
            }
        }

        public uint GetMessageCount()
        {
            var data = this.consumer.Model.QueueDeclarePassive("TimeoutMigrationTool_Staging");
            return data.MessageCount;
        }

        public void Dispose()
        {
            semaphore?.Dispose();
            messageProcessing?.Dispose();
            connection?.Dispose();
        }

        public async Task<int> CompleteBatch(int number)
        {
            Start(number);
            do
            {
                Thread.Sleep(100);
            } while (processedMessages < messageCount);

            if (QueueCreator.GetStatingQueueMessageLength(consumer.Model) > 0)
            {
                throw new InvalidOperationException("Staging queue is not empty after finishing CompleteBatch");
            }

            await Stop();
            return processedMessages;
        }

        ConnectionFactory factory;
        private readonly ILogger logger;
        string queueName;
    }
}