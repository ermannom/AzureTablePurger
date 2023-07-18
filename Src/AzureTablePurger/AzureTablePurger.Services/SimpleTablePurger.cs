using Azure;
using Azure.Data.Tables;
//using AzureTablePurger.Common.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace AzureTablePurger.Services
{
    public class SimpleTablePurger : ITablePurger
    {
        public const int MaxBatchSize = 100;
        public const int ConnectionLimit = 32;

        private readonly IAzureStorageClientFactory _storageClientFactory;
        private readonly ILogger<SimpleTablePurger> _logger;
        private readonly IPartitionKeyHandler _partitionKeyHandler;
        private readonly PurgeEntitiesOptions _options;

        public SimpleTablePurger(IOptions<PurgeEntitiesOptions> options, IAzureStorageClientFactory storageClientFactory, IPartitionKeyHandler partitionKeyHandler, ILogger<SimpleTablePurger> logger)
        {
            _storageClientFactory = storageClientFactory;
            _partitionKeyHandler = partitionKeyHandler;
            _logger = logger;
            _options = options.Value;

            ServicePointManager.DefaultConnectionLimit = ConnectionLimit;
        }

        public async Task<Tuple<int, int>> PurgeEntitiesAsync(CancellationToken cancellationToken)
        {
            var sw = new Stopwatch();
            sw.Start();

            _logger.LogInformation($"Starting PurgeEntitiesAsync");

            var tableServiceClient = _storageClientFactory.GetTableServiceClient(_options.TargetStorageAccountConnectionString);
            var tableClient = tableServiceClient.GetTableClient(_options.TargetTableName);

            _logger.LogInformation($"TargetAccount={tableServiceClient.AccountName}, Table={tableClient.Name}, PurgeRecordsOlderThanDays={_options.PurgeRecordsOlderThanDays}");

            var query = _partitionKeyHandler.GetTableQuery(_options.PurgeRecordsOlderThanDays);

            string continuationToken = null;
            bool moreResultsAvailable = true;
            int numPagesProcessed = 0;
            int numEntitiesDeleted = 0;

            do
            {
                Page<TableEntity> page = tableClient.Query<TableEntity>(filter: query, select: new List<string>() { "PartitionKey", "RowKey" })
                  .AsPages(continuationToken)
                  .FirstOrDefault(); // Note: Since the pageSizeHint only limits the number of results in a single page, we explicitly only enumerate the first page.
                var pageNumber = numPagesProcessed + 1;

                if (page == null && numPagesProcessed == 0)
                {
                    _logger.LogDebug($"No entities were available for purging");
                    break;
                }

                IReadOnlyList<TableEntity> pageResults = page.Values;

                var firstResultTimestamp = _partitionKeyHandler.ConvertPartitionKeyToDateTime(pageResults.First().PartitionKey);
                _logger.LogInformation($"Page {pageNumber}: processing {page.Values.Count()} results starting at timestamp {firstResultTimestamp}");

                var partitionsFromPage = GetPartitionsFromPage(pageResults);

                _logger.LogDebug($"Page {pageNumber}: number of partitions grouped by PartitionKey: {partitionsFromPage.Count}");

                var tasks = new List<Task<int>>();

                foreach (var partition in partitionsFromPage)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var chunkedPartition = partition.Chunk(MaxBatchSize).ToList();

                    foreach (var batch in chunkedPartition)
                    {
                        cancellationToken.ThrowIfCancellationRequested();


                        // Print out the results for this page.
                        //foreach (TableEntity result in pageResults)
                        //{
                        //    Console.WriteLine($"{result.PartitionKey}-{result.RowKey}");
                        //}

                        // Implementation 1: one at a time
                        //var recordsDeleted = await DeleteRecordsAsync(table, batch.ToList());
                        //numEntitiesDeleted += recordsDeleted;

                        // Implementation 2: all deletes asynchronously
                        tasks.Add(DeleteRecordsAsync(tableClient, batch.ToList()));
                    }
                }

                // Implementation 2: all deletes asynchronously
                // Wait for and consolidate results
                await Task.WhenAll(tasks);
                var numEntitiesDeletedInThisPage = tasks.Sum(t => t.Result);
                numEntitiesDeleted += numEntitiesDeletedInThisPage;
                _logger.LogDebug($"Page {pageNumber}: processing complete, {numEntitiesDeletedInThisPage} entities deleted");

                // Get the continuation token from the page.
                // Note: This value can be stored so that the next page query can be executed later.
                continuationToken = page.ContinuationToken;
                numPagesProcessed++;
                moreResultsAvailable = pageResults.Any() && continuationToken != null;

            } while (moreResultsAvailable);

            var entitiesPerSecond = numEntitiesDeleted > 0 ? (int)(numEntitiesDeleted / sw.Elapsed.TotalSeconds) : 0;
            var msPerEntity = numEntitiesDeleted > 0 ? (int)(sw.Elapsed.TotalMilliseconds / numEntitiesDeleted) : 0;

            _logger.LogInformation($"Finished PurgeEntitiesAsync, processed {numPagesProcessed} pages and deleted {numEntitiesDeleted} entities in {sw.Elapsed} ({entitiesPerSecond} entities per second, or {msPerEntity} ms per entity)");

            return new Tuple<int, int>(numPagesProcessed, numEntitiesDeleted);
        }

        /// <summary>
        /// Executes a batch delete
        /// </summary>
        private async Task<int> DeleteRecordsAsync(TableClient client, IList<TableEntity> batch)
        {
            if (batch.Count > MaxBatchSize)
            {
                throw new ArgumentException($"Batch size of {batch.Count} is larger than the maximum allowed size of {MaxBatchSize}");
            }

            var partitionKey = batch.First().PartitionKey;

            if (batch.Any(entity => entity.PartitionKey != partitionKey))
            {
                throw new ArgumentException($"Not all entities in the batch contain the same partitionKey");
            }

            _logger.LogTrace($"Deleting {batch.Count} rows from partitionKey={partitionKey}");

            // Create the batch.
            List<TableTransactionAction> deleteEntitiesBatch = new List<TableTransactionAction>();

            // Add the entities to be added to the batch.
            deleteEntitiesBatch.AddRange(batch.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e)));

            // Submit the batch.
            try
            {
                Response<IReadOnlyList<Response>> response = await client.SubmitTransactionAsync(deleteEntitiesBatch).ConfigureAwait(false);
                return batch.Count;
            }
            catch (RequestFailedException ex)
            {
                if (ex.ErrorCode.Equals("EntityNotFound") && (ex.Status == 404))
                {
                    _logger.LogWarning($"Failed to delete rows from partitionKey={partitionKey}. Data has already been deleted, ex.Message={ex.Message}, HttpStatusCode={ex.Status}, ErrorCode={ex.ErrorCode}");
                    return 0;
                }

                _logger.LogError($"Failed to delete rows from partitionKey={partitionKey}. Unknown error. ex.Message={ex.Message}, HttpStatusCode={ex.Status}, ErrorCode={ex.ErrorCode}");
                throw;
            }
        }

        /// <summary>
        /// Breaks up a result page into partitions grouped by PartitionKey
        /// </summary>
        private IList<IList<TableEntity>> GetPartitionsFromPage(IReadOnlyList<TableEntity> page)
        {
            var result = new List<IList<TableEntity>>();

            var groupByResult = page.GroupBy(x => x.PartitionKey);

            foreach (var partition in groupByResult.ToList())
            {
                var partitionAsList = partition.ToList();
                result.Add(partitionAsList);
            }

            return result;
        }
    }
}