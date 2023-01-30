namespace Particular.TimeoutMigrationTool.ASB
{
    using System;
    using System.Threading.Tasks;
    using Azure.Messaging.ServiceBus.Administration;
    using Microsoft.Extensions.Logging;
    using Microsoft.Identity.Client;

    public class AsbTarget : ITimeoutsTarget
    {
        readonly IAzureServiceBusEndpoint _azureServiceBusEndpoint;
        readonly ILogger logger;

        public AsbTarget(IAzureServiceBusEndpoint azureServiceBusEndpoint, ILogger logger)
        {
            _azureServiceBusEndpoint = azureServiceBusEndpoint;
            this.logger = logger;
        }

        public async ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            var migrationsResult = new MigrationCheckResult();

            try
            {
                await EnsureQueueExists(AsbConstants.MigrationQueue);
              //  await EnsureQueueExists(endpoint.EndpointName);
                var result = await _azureServiceBusEndpoint.GetQueueAsync(endpoint.EndpointName);
                //if (result.Status == EntityStatus.Active)
                //{
                //    return migrationsResult;
                //}
            }
            catch (Exception)
            {
                migrationsResult.Problems.Add($"Can not connect to queueName '{endpoint.EndpointName}' on connection ");
            }

            return migrationsResult;
        }

        async Task EnsureQueueExists(string queueName)
        {
            if (!await _azureServiceBusEndpoint.QueueExistsAsync(queueName))
            {
                await _azureServiceBusEndpoint.CreateQueueAsync(queueName);
            }
        }

        public ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(string endpointName)
        {
            return new ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator>(new AsbEndpointMigrator(_azureServiceBusEndpoint, endpointName, logger));
        }

        public ValueTask Abort(string endpointName)
        {
            return new ValueTask();
        }

        public ValueTask Complete(string endpointName) => new ValueTask(Task.CompletedTask);
    }
}