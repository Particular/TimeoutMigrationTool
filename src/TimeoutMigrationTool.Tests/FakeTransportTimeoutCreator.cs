using System.Collections.Generic;
using System.Threading.Tasks;
using Particular.TimeoutMigrationTool;

namespace TimeoutMigrationTool.Tests
{
    public class FakeTransportTimeoutCreator : ICreateTransportTimeouts
    {
        private List<string> problemsToReturn;
        public bool BatchWasStaged { get; private set; } = false;
        public List<TimeoutData> TimeoutsStaged { get; } = new List<TimeoutData>();
        public List<int> BatchesCompleted { get; } = new List<int>();
        public bool EndpointWasVerified { get; private set; } = false;

        public Task<int> StageBatch(List<TimeoutData> timeouts)
        {
            BatchWasStaged = true;
            TimeoutsStaged.AddRange(timeouts);
            return Task.FromResult(TimeoutsStaged.Count);
        }

        public Task<int> CompleteBatch(int number)
        {
            BatchesCompleted.Add(number);
            return Task.FromResult(BatchesCompleted.Count);
        }

        public Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            EndpointWasVerified = true;
            var problems = this.problemsToReturn ?? new List<string>();

            var result = new MigrationCheckResult
            {
                Problems = problems
            };
            return Task.FromResult(result);
        }

        public void SetupProblemsToReturn(List<string> problems)
        {
            this.problemsToReturn = problems;
        }
    }
}