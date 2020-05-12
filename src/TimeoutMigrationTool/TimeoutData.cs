using System;
using System.Collections.Generic;

namespace Particular.TimeoutMigrationTool
{
    class TimeoutData
    {
        //
        // Summary:
        //     Id of this timeout.
        public string Id { get; set; }
        //
        // Summary:
        //     The address of the client who requested the timeout.
        public string Destination { get; set; }
        //
        // Summary:
        //     The saga ID.
        public Guid SagaId { get; set; }
        //
        // Summary:
        //     Additional state.
        public byte[] State { get; set; }
        //
        // Summary:
        //     The time at which the timeout expires.
        public DateTime Time { get; set; }
        //
        // Summary:
        //     The timeout manager that owns this particular timeout.
        public string OwningTimeoutManager { get; set; }
        //
        // Summary:
        //     Store the headers to preserve them across timeouts.
        public Dictionary<string, string> Headers { get; set; }
    }
}
