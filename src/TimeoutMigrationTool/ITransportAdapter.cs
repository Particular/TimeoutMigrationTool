namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public interface ICreateTransportTimeouts
    {
        Task<int> StageBatch(List<TimeoutData> timeouts);
        Task<int> CompleteBatch(int number);
        Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint);
    }

    public class MigrationCheckResult
    {
        public List<string> Problems { get; set; } = new List<string>();

        public bool CanMigrate
        {
            get
            {
                return !Problems.Any();
            }
        }
    }
}