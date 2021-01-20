using NHibernate;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using NHibernate.Type;

namespace Particular.TimeoutMigrationTool.NHibernate
{
    class MigrationsEntityMap : ClassMapping<MigrationsEntity>
    {
        public MigrationsEntityMap()
        {
            Id(x => x.MigrationRunId);
            Property(p => p.EndpointName, pm => { pm.Length(500); });
            Property(p => p.Status, pm => pm.Type<EnumType<MigrationStatus>>());
            Property(p => p.RunParameters, pm => { pm.Length(4001); });
            Property(p => p.NumberOfBatches);
            Property(p => p.CutOffTime);
            Property(p => p.StartedAt);
            Property(p => p.CompletedAt);
        }
    }
}