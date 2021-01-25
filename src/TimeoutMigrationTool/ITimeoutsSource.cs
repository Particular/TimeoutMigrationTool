namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutsSource
    {
        /// <summary>
        /// Loads the current timeout migration or null if no migration in progress.
        /// </summary>
        Task<IToolState> TryLoadOngoingMigration();

        /// <summary>
        /// Creates a new timeout migration, moves all timeouts that match the specified endpoint name and cutoff time to a staged table, and breaks the timeouts up into batches
        /// </summary>
        Task<IToolState> Prepare(DateTime maxCutoffTime, string endpointName, IDictionary<string, string> runParameters);

        /// <summary>
        /// Reads all timeouts from a specified batch number
        /// </summary>
        Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber);

        /// <summary>
        /// Marks a batch as completed.
        /// </summary>
        Task MarkBatchAsCompleted(int number);

        /// <summary>
        /// Marks a batch as staged.
        /// </summary>
        Task MarkBatchAsStaged(int number);

        /// <summary>
        /// Lists all the endpoints found that have timeouts associated with them.
        /// </summary>
        Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTime cutOffTime);

        /// <summary>
        /// Aborts the migration and moves all staged timeouts back to the original location
        /// </summary>
        Task Abort();

        /// <summary>
        /// Sets the current migration status to Complete and sets the completed date to UtcNow.
        /// </summary>
        Task Complete();

        /// <summary>
        /// Returns true if a migration is in progress
        /// </summary>
        Task<bool> CheckIfAMigrationIsInProgress();
    }
}