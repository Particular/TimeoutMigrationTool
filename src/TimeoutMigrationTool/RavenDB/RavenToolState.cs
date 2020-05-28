using System.Collections.Generic;
using System.Linq;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenToolState
    {
        public List<string> Batches { get; set; } = new List<string>();
        public IDictionary<string, string> RunParameters { get; set; } = new Dictionary<string, string>();
        public MigrationStatus Status { get; set; }

        internal static RavenToolState FromToolState(ToolState toolState)
        {
            return new RavenToolState()
            {
                RunParameters = toolState.RunParameters,
                Status = toolState.Status,
                Batches = toolState.Batches.Select(b => $"{RavenConstants.BatchPrefix}/{b.Number}").ToList()
            };
        }

        internal ToolState ToToolState(List<BatchInfo> batches)
        {
            return new ToolState(batches, RunParameters, Status);
        }
    }
}