using System.Collections.Generic;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool
{
    public interface ITransportAdapter
    {
        Task StageBatch(List<TimeoutData> timeouts);
        Task CompleteBatch();
    }
}