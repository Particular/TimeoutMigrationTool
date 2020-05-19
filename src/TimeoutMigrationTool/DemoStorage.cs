namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class DemoStorage : ITimeoutStorage
    {
        public Task CompleteBatch(int number)
        {
            return Task.Delay(3000);
        }

        public Task<ToolState> GetOrCreateToolState()
        {
            toolState = new ToolState();

            return Task.FromResult(toolState);
        }

        public async Task<List<BatchInfo>> Prepare()
        {
            await Task.Delay(3000).ConfigureAwait(false);

            return new List<BatchInfo> { new BatchInfo { Number = 1 }, new BatchInfo { Number = 2 }, new BatchInfo { Number = 3 } };
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            await Task.Delay(3000).ConfigureAwait(false);

            return new List<TimeoutData> { new TimeoutData(), new TimeoutData() };
        }

        public Task StoreToolState(ToolState newToolState)
        {
            return Task.CompletedTask;
        }

        ToolState toolState;
    }
}