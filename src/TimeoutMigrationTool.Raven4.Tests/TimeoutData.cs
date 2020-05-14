using System;
using System.Collections.Generic;

namespace TimeoutMigrationTool.Raven4.Tests
{
    class TimeoutData
    {
        public TimeoutData()
        {
        }
        
        /// <summary>
        ///     The address of the client who requested the timeout.
        /// </summary>
        public string Destination { get; set; }

        /// <summary>
        ///     The saga ID.
        /// </summary>
        public Guid SagaId { get; set; }

        /// <summary>
        ///     Additional state.
        /// </summary>
        public byte[] State { get; set; }

        /// <summary>
        ///     The time at which the timeout expires.
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        ///     The timeout manager that owns this particular timeout
        /// </summary>
        public string OwningTimeoutManager { get; set; }

        /// <summary>
        ///     Store the headers to preserve them across timeouts
        /// </summary>
        public Dictionary<string, string> Headers { get; set; }
        
    }
}