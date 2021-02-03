namespace Particular.TimeoutMigrationTool.NoOp
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class NoOpTarget : ITimeoutsTarget, ITimeoutsTarget.IEndpointTarget
    {
        int lastStaged;

        public ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint) => new ValueTask<MigrationCheckResult>(new MigrationCheckResult());

        public ValueTask<ITimeoutsTarget.IEndpointTarget> Migrate(string endpointName) => new ValueTask<ITimeoutsTarget.IEndpointTarget>(this);

        public ValueTask Abort(string endpointName) => new ValueTask();

        public ValueTask Complete(string endpointName) => new ValueTask();

        public ValueTask DisposeAsync() => new ValueTask();

        public ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            lastStaged = timeouts.Count;
            return new ValueTask<int>(lastStaged);
        }

        public ValueTask<int> CompleteBatch(int batchNumber) => new ValueTask<int>(lastStaged);
    }
}