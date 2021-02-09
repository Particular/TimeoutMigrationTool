namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ICanTalkToRavenVersion
    {
        Task UpdateDocument(string key, object document);
        Task DeleteDocument(string key);
        Task CreateBatchAndUpdateTimeouts(RavenBatch batch);
        Task DeleteBatchAndUpdateTimeouts(RavenBatch batch);
        Task BatchDelete(string[] keys);
        Task<IReadOnlyList<T>> GetDocuments<T>(Func<T, bool> filterPredicate, string documentPrefix, Action<T, string> idSetter = null, int pageSize = RavenConstants.DefaultPagingSize) where T : class;
        Task<IReadOnlyList<T>> GetPagedDocuments<T>(string documentPrefix, int startFrom, Action<T, string> idSetter = null, int nrOfPages = 0) where T : class;
        Task<T> GetDocument<T>(string id, Action<T, string> idSetter = null) where T : class;
        Task<IReadOnlyList<T>> GetDocuments<T>(IEnumerable<string> ids, Action<T, string> idSetter = null) where T : class;
        Task CompleteBatchAndUpdateTimeouts(RavenBatch batch);
        Task<GetByIndexResult<T>> GetDocumentsByIndex<T>(int startFrom, TimeSpan timeToWaitForNonStaleResults, Action<T, string> idSetter = null) where T : class;
        Task ArchiveDocument(string archivedToolStateId, RavenToolStateDto ravenToolState);
    }
}