namespace TimeoutMigrationTool.Tests
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool;

    public class FakeTimeoutTarget : ITimeoutsTarget, ITimeoutsTarget.IEndpointTarget
    {
        private List<string> problemsToReturn;
        public bool BatchWasStaged { get; private set; }
        public List<TimeoutData> TimeoutsStaged { get; } = new List<TimeoutData>();
        public List<int> BatchesCompleted { get; } = new List<int>();
        public bool EndpointWasVerified { get; private set; } = false;

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
            var problems = problemsToReturn ?? new List<string>();

            var result = new MigrationCheckResult
            {
                Problems = problems
            };
            return new ValueTask<MigrationCheckResult>(result);
        }

        public ValueTask<ITimeoutsTarget.IEndpointTarget> Migrate(EndpointInfo endpoint)
        {
            return new ValueTask<ITimeoutsTarget.IEndpointTarget>(this);
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask();
        }

        public void SetupProblemsToReturn(List<string> problems)
        {
            this.problemsToReturn = problems;
        }
    }
}