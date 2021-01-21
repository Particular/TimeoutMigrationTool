namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutsTarget
    {
        ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint);

        ValueTask<IEndpointTarget> Migrate(string endpointName);
        ValueTask Abort(string endpointName);

        public interface IEndpointTarget : IAsyncDisposable
        {
            ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber);
            ValueTask<int> CompleteBatch(int batchNumber);

        }
    }
}