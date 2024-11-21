namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class RavenToolStateDto
    {
        public List<string> Batches { get; set; } = [];
        public IDictionary<string, string> RunParameters { get; set; } = new Dictionary<string, string>();
        public MigrationStatus Status { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime CompletedAt { get; set; }
        public string Endpoint { get; set; }
        public int NumberOfBatches { get; set; }
        public int NumberOfTimeouts { get; set; }

        public static List<string> ToBatches(IEnumerable<RavenBatch> batches)
        {
            return batches.Select(b => $"{RavenConstants.BatchPrefix}/{b.Number}").ToList();
        }

        public RavenToolState ToToolState(IEnumerable<RavenBatch> batches)
        {
            return new RavenToolState(RunParameters, Endpoint, batches, Status);
        }
    }
}