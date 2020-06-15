using System.Collections.Generic;
using System.Linq;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    internal class RavenToolStateDto
    {
        public List<string> Batches { get; set; } = new List<string>();
        public IDictionary<string, string> RunParameters { get; set; } = new Dictionary<string, string>();
        public MigrationStatus Status { get; set; }

        public string Endpoint { get;  set; }

        internal static RavenToolStateDto FromToolState(RavenToolState toolState)
        {
            return new RavenToolStateDto()
            {
                RunParameters = toolState.RunParameters,
                Batches = toolState.Batches.Select(b => $"{RavenConstants.BatchPrefix}/{b.Number}").ToList(),
                Endpoint = toolState.EndpointName
            };
        }

        internal RavenToolState ToToolState(List<RavenBatch> batches)
        {
            return new RavenToolState(RunParameters, Endpoint, batches);
        }
    }
}