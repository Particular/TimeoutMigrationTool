namespace Particular.TimeoutMigrationTool.ASQ
{
    using Microsoft.Azure.Cosmos.Table;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    public static class TableExtensions
    {
        public static async Task<IList<T>> ExecuteQueryAsync<T>(this CloudTable table, TableQuery<T> query, CancellationToken cancellationToken)
            where T : ITableEntity, new()
        {
            var items = new List<T>();
            TableContinuationToken token = null;

            do
            {
                var seg = await table.ExecuteQuerySegmentedAsync(query, token, null, null, cancellationToken).ConfigureAwait(false);
                token = seg.ContinuationToken;
                items.AddRange(seg);
            }
            while (token != null && !cancellationToken.IsCancellationRequested);

            return items;
        }
    }
}
