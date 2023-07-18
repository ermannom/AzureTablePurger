using AzureTablePurger.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;


namespace Microsoft.Extensions.DependencyInjection
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddConfig(
             this IServiceCollection services, IConfiguration config)
        {
            services.Configure<PurgeEntitiesOptions>(
                config.GetSection(PurgeEntitiesOptions.PurgeEntitiesSection));

            return services;
        }

        public static IServiceCollection AddPurgeServices(
             this IServiceCollection services)
        {
            // Core logic
            services.AddScoped<ITablePurger, SimpleTablePurger>();
            services.AddScoped<IAzureStorageClientFactory, AzureStorageClientFactory>();
            services.AddScoped<IPartitionKeyHandler, PartitionKeyHandler>();

            return services;
        }

        public static IServiceCollection ConfigureLogging(this IServiceCollection services)
        {
            services.AddLogging(configure =>
                    configure.AddSimpleConsole(options =>
                    {
                        options.SingleLine = true;
                        options.ColorBehavior = LoggerColorBehavior.Default;
                        options.IncludeScopes = false;
                        options.TimestampFormat = "[yyyy-MM-dd HH:mm:ss:fffff tt] ";
                    }))
                .Configure<LoggerFilterOptions>(options => options.MinLevel = LogLevel.Debug);

            return services;
        }
    }
}
