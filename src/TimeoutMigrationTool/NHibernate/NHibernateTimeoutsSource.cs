namespace Particular.TimeoutMigrationTool.NHibernate
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using global::NHibernate;
    using global::NHibernate.Cfg;
    using global::NHibernate.Cfg.MappingSchema;
    using global::NHibernate.Criterion;
    using global::NHibernate.Mapping.ByCode;
    using Newtonsoft.Json;

    public class NHibernateTimeoutsSource : ITimeoutsSource
    {
        public NHibernateTimeoutsSource(string connectionString, int batchSize, DatabaseDialect databaseDialect)
        {
            this.connectionString = connectionString;
            this.batchSize = batchSize;
            this.databaseDialect = databaseDialect;
        }

        ISessionFactory CreateSessionFactory()
        {
            var cfg = new Configuration().DataBaseIntegration(x =>
                {
                    x.ConnectionString = connectionString;
                    x.LogSqlInConsole = true;
                });

            databaseDialect.ConfigureDriverAndDialect(cfg);

            var mapper = new ModelMapper();
            mapper.AddMapping<TimeoutEntityMap>();
            mapper.AddMapping<StagedTimeoutEntityMap>();
            mapper.AddMapping<MigrationsEntityMap>();

            HbmMapping mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            cfg.AddMapping(mapping);
            cfg.SetProperty("hbm2ddl.auto", "update");

            return cfg.BuildSessionFactory();
        }

        public async Task<IToolState> TryLoadOngoingMigration()
        {
            using var session = CreateSessionFactory().OpenSession();

            var migration = await session.QueryOver<MigrationsEntity>()
                .Where(m => m.Status == MigrationStatus.StoragePrepared)
                .SingleOrDefaultAsync();

            if (migration == null)
            {
                return null;
            }

            async Task<BatchInfo> GetNextBatch()
            {
                using var session = CreateSessionFactory().OpenStatelessSession();

                var stagedTimeoutEntities = await session.QueryOver<StagedTimeoutEntity>()
                    .Where(stagedTimeoutEntity => stagedTimeoutEntity.BatchState != BatchState.Completed)
                    .SelectList(timeoutEntities => timeoutEntities.SelectGroup(te => te.BatchNumber)
                        .SelectGroup(te => te.BatchState)
                        .SelectCount(te => te.Id))
                    .OrderBy(entity => entity.BatchNumber)
                    .Asc.ListAsync<object[]>();

                return stagedTimeoutEntities.Select(x => new BatchInfo((int)x[0], (BatchState)x[1], (int)x[2]))
                    .FirstOrDefault();
            }

            return new NHibernateToolState(GetNextBatch, migration.MigrationRunId, JsonConvert.DeserializeObject<Dictionary<string, string>>(migration.RunParameters), migration.EndpointName, migration.NumberOfBatches);
        }

        public async Task<IToolState> Prepare(DateTime maxCutoffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            using var session = CreateSessionFactory().OpenSession();
            using var tx = session.BeginTransaction();

            var copyTimeoutsToStagedQuery = session.CreateQuery(@"
INSERT INTO StagedTimeoutEntity (
    Id,
    Destination,
    SagaId,
    State,
    Endpoint,
    Time,
    Headers,
    BatchNumber,
    BatchState
)
SELECT
    TE.Id,
    TE.Destination,
    TE.SagaId,
    TE.State,
    TE.Endpoint,
    TE.Time,
    TE.Headers,
    -1,
    :BatchPendingStatus
FROM TimeoutEntity TE
WHERE TE.Time >= :CutOffTime AND TE.Endpoint = :EndpointName;
");
            copyTimeoutsToStagedQuery.SetParameter("EndpointName", endpointName);
            copyTimeoutsToStagedQuery.SetParameter("CutOffTime", maxCutoffTime);
            copyTimeoutsToStagedQuery.SetParameter("BatchPendingStatus", BatchState.Pending);

            await copyTimeoutsToStagedQuery.ExecuteUpdateAsync();

            var deleteTimeoutsThatHaveBeenStagedQuery = session.CreateQuery(@"
DELETE TimeoutEntity TE
WHERE TE.Time >= :CutOffTime AND TE.Endpoint = :EndpointName;");
            deleteTimeoutsThatHaveBeenStagedQuery.SetParameter("EndpointName", endpointName);
            deleteTimeoutsThatHaveBeenStagedQuery.SetParameter("CutOffTime", maxCutoffTime);

            await deleteTimeoutsThatHaveBeenStagedQuery.ExecuteUpdateAsync();

            var breakStagedTimeoutsIntoBatchesSqlQuery = session.CreateSQLQuery(databaseDialect.GetSqlToBreakStagedTimeoutsIntoBatches(batchSize));
            await breakStagedTimeoutsIntoBatchesSqlQuery.ExecuteUpdateAsync();

            var maxBatchNumber = await session.QueryOver<StagedTimeoutEntity>().Select(Projections.Max<StagedTimeoutEntity>(stagedTimeout => stagedTimeout.BatchNumber))
                .SingleOrDefaultAsync<int>();

            var migrationRun = new MigrationsEntity
            {
                MigrationRunId = Guid.NewGuid().ToString().Replace("-", ""),
                EndpointName = endpointName,
                Status = MigrationStatus.StoragePrepared,
                RunParameters = JsonConvert.SerializeObject(runParameters),
                NumberOfBatches = maxBatchNumber,
                CutOffTime = maxCutoffTime,
                StartedAt = DateTime.UtcNow
            };

            await session.SaveOrUpdateAsync(migrationRun);

            await tx.CommitAsync();

            return await TryLoadOngoingMigration();
        }

        public async Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber)
        {
            using var session = CreateSessionFactory().OpenStatelessSession();

            var timeouts = await session.QueryOver<StagedTimeoutEntity>()
                .Where(timeout => timeout.BatchNumber == batchNumber)
                .ListAsync();

            return timeouts.Select(timeout => timeout.ToTimeoutData()).ToList();
        }

        public async Task MarkBatchAsCompleted(int number)
        {
            using var session = CreateSessionFactory().OpenSession();
            using var tx = session.BeginTransaction();

            var markBatchAsCompletedSql = session.CreateQuery($@"
UPDATE StagedTimeoutEntity STE
SET STE.BatchState = :BatchCompletedStatus
WHERE STE.BatchNumber = :BatchNumber
");

            markBatchAsCompletedSql.SetParameter("BatchCompletedStatus", BatchState.Completed);
            markBatchAsCompletedSql.SetParameter("BatchNumber", number);

            await markBatchAsCompletedSql.ExecuteUpdateAsync();

            await tx.CommitAsync();
        }

        public async Task MarkBatchAsStaged(int number)
        {
            using var session = CreateSessionFactory().OpenSession();
            using var tx = session.BeginTransaction();

            var markBatchAsStagedSql = session.CreateQuery($@"
UPDATE StagedTimeoutEntity STE
SET STE.BatchState = :BatchCompletedStatus
WHERE STE.BatchNumber = :BatchNumber;
");

            markBatchAsStagedSql.SetParameter("BatchCompletedStatus", BatchState.Staged);
            markBatchAsStagedSql.SetParameter("BatchNumber", number);

            await markBatchAsStagedSql.ExecuteUpdateAsync();

            await tx.CommitAsync();
        }

        public async Task Abort()
        {
            var migrationRun = await TryLoadOngoingMigration();

            using var session = CreateSessionFactory().OpenSession();
            using var tx = session.BeginTransaction();

            var copyStagedTimeoutsBackToTimeoutsHqlQuery = session.CreateQuery(@"
INSERT INTO TimeoutEntity (
    Id,
    Destination,
    SagaId,
    State,
    Endpoint,
    Time,
    Headers
)
SELECT
    STE.Id,
    STE.Destination,
    STE.SagaId,
    STE.State,
    STE.Endpoint,
    STE.Time,
    STE.Headers
FROM StagedTimeoutEntity STE
WHERE STE.BatchState <> :BatchCompletedStatus;
");
            copyStagedTimeoutsBackToTimeoutsHqlQuery.SetParameter("BatchCompletedStatus", BatchState.Completed);
            await copyStagedTimeoutsBackToTimeoutsHqlQuery.ExecuteUpdateAsync();

            var deleteStagedTimeoutsHqlQuery = session.CreateQuery(@"
DELETE StagedTimeoutEntity STE
WHERE STE.BatchState <> :BatchCompletedStatus;");
            deleteStagedTimeoutsHqlQuery.SetParameter("BatchCompletedStatus", BatchState.Completed);
            await deleteStagedTimeoutsHqlQuery.ExecuteUpdateAsync();

            var markMigrationEntityAsAbortedHqlQuery = session.CreateQuery(@"UPDATE MigrationsEntity
SET
    CompletedAt= :CompletedAt,
    Status = :AbortedStatus
WHERE
    MigrationRunId = :MigrationRunId;");
            markMigrationEntityAsAbortedHqlQuery.SetParameter("CompletedAt", DateTime.UtcNow);
            markMigrationEntityAsAbortedHqlQuery.SetParameter("AbortedStatus", MigrationStatus.Aborted);
            markMigrationEntityAsAbortedHqlQuery.SetParameter("MigrationRunId", ((NHibernateToolState)migrationRun).MigrationRunId);
            await markMigrationEntityAsAbortedHqlQuery.ExecuteUpdateAsync();

            await tx.CommitAsync();
        }

        public async Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTime cutOffTime)
        {
            using var session = CreateSessionFactory().OpenStatelessSession();

            TimeoutEntity timeoutAlias = null;
            var timeouts = await session.QueryOver(() => timeoutAlias)
                .Where(timeout => timeout.Time >= cutOffTime)
                .SelectList(list => list
                    .SelectGroup(p => p.Endpoint)
                    .SelectMax(p => p.Time)
                    .SelectMin(p => p.Time)
                    .SelectCount(p => p.Id)
                    ).ListAsync<object[]>();

            return timeouts.Select(x => new EndpointInfo
            {
                EndpointName = x[0].ToString(),
                LongestTimeout = (DateTime)x[1],
                ShortestTimeout = (DateTime)x[2],
                NrOfTimeouts = (int)x[3],
                Destinations = session.QueryOver<TimeoutEntity>()
                                            .Where(timeout => timeout.Endpoint == x[0].ToString())
                                            .SelectList(t => t.SelectGroup(x => x.Destination))
                                            .List<string>()
                                            .ToList()
            }).ToList();
        }

        public async Task Complete()
        {
            using var session = CreateSessionFactory().OpenSession();
            using var tx = session.BeginTransaction();

            var migration = await session.QueryOver<MigrationsEntity>()
                                    .Where(m => m.Status == MigrationStatus.StoragePrepared)
                                    .SingleOrDefaultAsync();

            migration.Status = MigrationStatus.Completed;
            migration.CompletedAt = DateTime.UtcNow;

            await session.UpdateAsync(migration);

            await tx.CommitAsync();
        }

        public async Task<bool> CheckIfAMigrationIsInProgress()
        {
            var toolState = await TryLoadOngoingMigration();
            return toolState != null;
        }

        readonly string connectionString;
        readonly int batchSize;
        DatabaseDialect databaseDialect;
    }
}