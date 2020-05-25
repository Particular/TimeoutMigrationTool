namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class DemoStorage : ITimeoutStorage
    {
        public Task CompleteBatch(int number)
        {
            return Task.Delay(3000);
        }

        public Task<ToolState> GetToolState()
        {
            return Task.FromResult((ToolState)null);
        }

        public async Task<List<BatchInfo>> Prepare(DateTime cutOffDate)
        {
            await Task.Delay(3000).ConfigureAwait(false);

            return new List<BatchInfo> { new BatchInfo { Number = 1 }, new BatchInfo { Number = 2 }, new BatchInfo { Number = 3 } };
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            await Task.Delay(3000).ConfigureAwait(false);

            return new List<TimeoutData> { new TimeoutData(), new TimeoutData() };
        }

        public Task Reset()
        {
            throw new NotImplementedException();
        }

        public Task StoreToolState(ToolState newToolState)
        {
            return Task.CompletedTask;
        }
    }
}