using AzureTablePurger.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AzureTablePurger.App
{
    class Program
    {
        private const string ConfigKeyTargetStorageAccountConnectionString = "TargetStorageAccountConnectionString";
        private const string ConfigKeyTargetTableName = "TargetTableName";
        private const string ConfigKeyPurgeRecordsOlderThanDays = "PurgeRecordsOlderThanDays";

        private static IServiceProvider _serviceProvider;
        private static IConfigurationRoot _config;

        static async Task Main(string[] args)
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
            builder.Configuration.AddUserSecrets<Program>();

            builder.Services.AddConfig(builder.Configuration);
            builder.Services.AddPurgeServices();
            builder.Services.ConfigureLogging();
            BuildConfig(builder, args);

            _serviceProvider = builder.Services.BuildServiceProvider();

            var logger = _serviceProvider.GetService<ILogger<Program>>();
            logger.LogInformation("Starting...");

            var tablePurger = _serviceProvider.GetService<ITablePurger>();

            var cts = new CancellationTokenSource();
            await tablePurger.PurgeEntitiesAsync(cts.Token);

            logger.LogInformation($"Finished");

            IHost host = builder.Build();
            host.Run();
        }

        private static void BuildConfig(HostApplicationBuilder builder, string[] commandLineArgs)
        {
            // Command line config
            var switchMapping = new Dictionary<string, string>
            {
                { "-account", ConfigKeyTargetStorageAccountConnectionString },
                { "-table", ConfigKeyTargetTableName },
                { "-days", ConfigKeyPurgeRecordsOlderThanDays }
            };

            builder.Configuration.AddCommandLine(commandLineArgs, switchMapping);
        }


    }
}