namespace TimeoutMigrationTool.Tests
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool;

    public class FakeTimeoutTarget : ITimeoutsTarget, ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        List<string> problemsToReturn;
        public bool BatchWasStaged { get; private set; }
        public List<TimeoutData> TimeoutsStaged { get; } = [];
        public List<int> BatchesCompleted { get; } = [];
        public bool EndpointWasVerified { get; private set; } = false;

        public bool MigrationWasAborted { get; private set; }
        public bool MigrationWasCompleted { get; private set; }


        public ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            BatchWasStaged = true;
            TimeoutsStaged.AddRange(timeouts);
            return new ValueTask<int>(TimeoutsStaged.Count);
        }

        public ValueTask<int> CompleteBatch(int batchNumber)
        {
            BatchesCompleted.Add(batchNumber);
            return new ValueTask<int>(BatchesCompleted.Count);
        }

        public ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            EndpointWasVerified = true;
            var problems = problemsToReturn ?? [];

            var result = new MigrationCheckResult
            {
                Problems = problems
            };
            return new ValueTask<MigrationCheckResult>(result);
        }

        public ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(string endpointName)
        {
            return new ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator>(this);
        }

        public ValueTask Abort(string endpointName)
        {
            MigrationWasAborted = true;
            return new ValueTask();
        }

        public ValueTask Complete(string endpointName)
        {
            MigrationWasCompleted = true;
            return new ValueTask();
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask();
        }

        public void SetupProblemsToReturn(List<string> problems)
        {
            problemsToReturn = problems;
        }
    }
}