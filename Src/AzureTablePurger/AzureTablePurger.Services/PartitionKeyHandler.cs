using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;

namespace AzureTablePurger.Services
{
    public class PartitionKeyHandler : IPartitionKeyHandler
    {
        private readonly ILogger<PartitionKeyHandler> _logger;
        private readonly string PartitionKeyFormat;
        public PartitionKeyHandler(ILogger<PartitionKeyHandler> logger, IOptions<PurgeEntitiesOptions> options)
        {
            _logger = logger;
            PartitionKeyFormat = options.Value.PartitionKeyFormat;
        }

        public string GetTableQuery(int purgeEntitiesOlderThanDays)
        {
            var maximumPartitionKeyToDelete = GetMaximumPartitionKeyToDelete(purgeEntitiesOlderThanDays);

            return GetTableQuery(null, maximumPartitionKeyToDelete);
        }

        public string GetTableQuery(string lowerBoundPartitionKey, string upperBoundPartitionKey)
        {
            if (string.IsNullOrEmpty(lowerBoundPartitionKey))
            {
                lowerBoundPartitionKey = "1970-01-01";
            }

            var lowerBoundDateTime = ConvertPartitionKeyToDateTime(lowerBoundPartitionKey);
            var upperBoundDateTime = ConvertPartitionKeyToDateTime(upperBoundPartitionKey);
            _logger.LogDebug($"Generating table query: lowerBound partitionKey={lowerBoundPartitionKey} ({lowerBoundDateTime}), upperBound partitionKey={upperBoundPartitionKey} ({upperBoundDateTime})");

            return TableClient.CreateQueryFilter($"PartitionKey ge {lowerBoundPartitionKey} and PartitionKey le {upperBoundPartitionKey}");

            //.Select(new[] { "PartitionKey", "RowKey" });

        }

        public DateTime ConvertPartitionKeyToDateTime(string partitionKey)
        {
            var result = long.TryParse(partitionKey, out long ticks);

            if (result)
            {
                new DateTime(ticks);
                //throw new ArgumentException($"PartitionKey is not in the expected format: {partitionKey}", nameof(partitionKey));
            }
            else
            {
                return DateTime.ParseExact(partitionKey, "yyyy-MM-dd", null);
            }

            return new DateTime(ticks);
        }

        public string GetPartitionKeyForDate(DateTime date)
        {
            return date.ToString("yyyy-MM-dd");
        }

        private string GetMaximumPartitionKeyToDelete(int purgeRecordsOlderThanDays)
        {
            return GetPartitionKeyForDate(DateTime.UtcNow.AddDays(-1 * purgeRecordsOlderThanDays));
        }
    }
}