using NHibernate;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using NHibernate.Type;

namespace Particular.TimeoutMigrationTool.NHibernate
{
    public class StagedTimeoutEntityMap : ClassMapping<StagedTimeoutEntity>
    {
        public StagedTimeoutEntityMap()
        {
            Id(x => x.Id, pm => pm.Generator(Generators.Assigned));
            Property(p => p.Destination, pm => { pm.Length(1024); });
            Property(p => p.SagaId, pm => pm.Index("StagedTimeoutEntity_SagaIdIdx"));
            Property(p => p.State, pm =>
            {
                pm.Type(NHibernateUtil.BinaryBlob);
                pm.Length(int.MaxValue);
            });
            Property(p => p.Endpoint, pm =>
            {
                pm.Index(EndpointIndexName);
                pm.Length(440);
            });
            Property(p => p.Time, pm => pm.Index(EndpointIndexName));
            Property(p => p.Headers, pm => pm.Type(NHibernateUtil.StringClob));

            Property(p => p.BatchState, pm => pm.Type<EnumType<BatchState>>());
            Property(p => p.BatchNumber);
        }

        public const string EndpointIndexName = "StagedTimeoutEntity_EndpointIdx";
    }
}