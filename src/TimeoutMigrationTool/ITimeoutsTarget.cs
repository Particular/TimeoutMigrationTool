namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutsTarget
    {
        /// <summary>
        /// Validates if the infrastructure is correctly configured to be able to migrate timeouts for a specific endpoint. Should check if:
        ///  - Can connect using the provided connection string
        ///  - The expected timeout queue exists
        ///  - The staging queue can be created using the provided credentials
        /// </summary>
        ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint);

        /// <summary>
        /// Prepares the staging queue for the target endpoint and returns an instance of a batch migrator for that endpoint.
        /// The batch migrator is responsible for adding timeouts to the staging queue in batches and moving messages from the staging
        /// queue to the final delayed delivery queues in batches.
        /// </summary>
        ValueTask<IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(string endpointName);


        /// <summary>
        /// Removes any target staging infrastructure that was created
        /// </summary>
        ValueTask Abort(string endpointName);

        /// <summary>
        /// Removes any target staging infrastructure that was created. Throws an exception if there are still records in the staging queues.
        /// </summary>
        ValueTask Complete(string endpointName);

        interface IEndpointTargetBatchMigrator : IAsyncDisposable
        {
            /// <summary>
            /// Adds the provided set of timeouts to the staging queue for timeouts
            /// </summary>
            ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber);

            /// <summary>
            /// Moves the specified batch from the staging queue to the final delayed delivery queue
            /// </summary>
            ValueTask<int> CompleteBatch(int batchNumber);
        }
    }
}