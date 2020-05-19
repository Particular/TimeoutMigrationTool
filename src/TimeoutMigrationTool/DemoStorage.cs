namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class DemoStorage : ITimeoutStorage
    {
        public DemoStorage()
        {
        }

        public Task CompleteBatch(int number)
        {
            return Task.CompletedTask;
        }

        public Task<ToolState> GetOrCreateToolState()
        {
            toolState = new ToolState();

            return Task.FromResult(toolState);
        }

        public Task<List<BatchInfo>> Prepare()
        {
            return Task.FromResult(new List<BatchInfo> { new BatchInfo { Number = 1 }, new BatchInfo { Number = 2 }, new BatchInfo { Number = 3 } });
        }

        public Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            return Task.FromResult(new List<TimeoutData> { new TimeoutData(), new TimeoutData() });
        }

        public Task StoreToolState(ToolState newToolState)
        {
            return Task.CompletedTask;
        }

        ToolState toolState;
    }
}