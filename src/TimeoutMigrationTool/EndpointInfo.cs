using System;

namespace Particular.TimeoutMigrationTool
{
    public class EndpointInfo
    {
        public string EndpointName { get; set; }
        public int NrOfTimeouts { get; set; }
        public DateTime LongestTimeout { get; set; }
        public DateTime ShortestTimeout { get; set; }
    }
}