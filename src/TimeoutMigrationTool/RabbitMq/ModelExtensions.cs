using System.Threading;

namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System.Threading.Tasks;
    using global::RabbitMQ.Client;
    using global::RabbitMQ.Client.Exceptions;

    static class ModelExtensions
    {
        class MessageState
        {
            public IModel Channel { get; set; }

            public ulong DeliveryTag { get; set; }
        }

        public static Task BasicAckSingle(this IModel channel, ulong deliveryTag, TaskScheduler scheduler) =>
            Task.Factory.StartNew(state =>
                {
                    var messageState = (MessageState) state;
                    messageState.Channel.BasicAck(messageState.DeliveryTag, false);
                }, new MessageState {Channel = channel, DeliveryTag = deliveryTag}, CancellationToken.None,
                TaskCreationOptions.DenyChildAttach, scheduler);

        public static Task
            BasicRejectAndRequeueIfOpen(this IModel channel, ulong deliveryTag, TaskScheduler scheduler) =>
            Task.Factory.StartNew(
                state =>
                {
                    var messageState = (MessageState) state;

                    if (messageState.Channel.IsOpen)
                    {
                        try
                        {
                            messageState.Channel.BasicReject(messageState.DeliveryTag, true);
                        }
                        catch (AlreadyClosedException)
                        {
                        }
                    }
                },
                new MessageState {Channel = channel, DeliveryTag = deliveryTag},
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                scheduler);
    }
}