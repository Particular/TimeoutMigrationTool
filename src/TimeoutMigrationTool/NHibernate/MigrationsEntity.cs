namespace Particular.TimeoutMigrationTool.NHibernate
{
    using System;

    class MigrationsEntity
    {
        public virtual string MigrationRunId { get; set; }
        public virtual string EndpointName { get; set; }
        public virtual MigrationStatus Status { get; set; }
        public virtual string RunParameters { get; set; }
        public virtual int NumberOfBatches { get; set; }
        public virtual DateTime CutOffTime { get; set; }
        public virtual DateTime StartedAt { get; set; }
        public virtual DateTime? CompletedAt { get; set; }
    }
}