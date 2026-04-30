using System.Text.Json;
using Application;
using Domain.Configuration;
using Domain.Interfaces;
using Infrastructure.Messaging;
using Infrastructure.Monitoring;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;

namespace Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddRabbitMqInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddApplication();
        
        // Bind настроек
        var rabbitMqSettings = configuration
                                   .GetSection(RabbitMqSettings.SectionName)
                                   .Get<RabbitMqSettings>() 
                               ?? throw new InvalidOperationException("RabbitMQ settings not configured");

        services.AddSingleton(rabbitMqSettings);
        services.AddSingleton<ISerializer, JsonMessageSerializer>();

        // Регистрация IConnection как singleton (RabbitMQ.Client thread-safe)
        services.AddSingleton<IConnection>(sp =>
        {
            var settings = sp.GetRequiredService<RabbitMqSettings>();
            var factory = new ConnectionFactory
            {
                HostName = settings.HostName,
                Port = settings.Port,
                UserName = settings.UserName,
                Password = settings.Password,
                VirtualHost = settings.VirtualHost,
                // Для продакшена:
                // AutomaticRecoveryEnabled = true,
                // NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
            };
            return factory.CreateConnection();
        });
        
        // 🔹 Bind Polly настроек
        var pollySettings = configuration
            .GetSection(PollySettings.SectionName)
            .Get<PollySettings>() ?? new PollySettings();

        services.AddSingleton(pollySettings);

        // 🔹 Регистрация ResiliencePipeline (Polly v8+)
        services.AddSingleton<ResiliencePipeline>(sp =>
        {
            if (!pollySettings.Retry.Enabled)
            {
                return new ResiliencePipelineBuilder().Build(); // Пустой пайплайн (NoOp)
            }

            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger("Polly.Retry");

            return new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    ShouldHandle = new PredicateBuilder().Handle<Exception>(), // ⚠️ В продакшене: .Handle<HttpRequestException>()
                    MaxRetryAttempts = pollySettings.Retry.RetryCount,
                    DelayGenerator = context =>
                    {
                        var delay = pollySettings.Retry.InitialDelaySeconds;
                        var actualDelay = pollySettings.Retry.UseExponentialBackoff
                            ? delay * Math.Pow(2, context.AttemptNumber) // AttemptNumber начинается с 0
                            : delay;
                        
                        return new ValueTask<TimeSpan?>(TimeSpan.FromSeconds(actualDelay));
                    },
                    OnRetry = (outcome) =>
                    {
                        logger.LogWarning(outcome.Outcome.Exception, 
                            "⏳ Retry attempt {Attempt}/{MaxAttempts} after {Delay}s | Exception: {ExceptionType}", 
                            outcome.AttemptNumber + 1, // +1 потому что начинается с 0
                            pollySettings.Retry.RetryCount,
                            outcome.RetryDelay.TotalSeconds,
                            outcome.Outcome.Exception?.GetType().Name);
                        
                        return default;
                    }
                })
                .Build();
        });
        
        // Регистрация фоновых сервисов
        services.AddTransient<IMessageProducer, RabbitMqProducer>();
        services.AddHostedService<InputQueueConsumer>();
        services.AddHostedService<OutputQueueConsumer>();
        
        services.AddHealthChecks()
            .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready", "live"]);
        
        // Регистрация фоновой задачи сбора метрик глубины очередей
        services.AddHostedService<QueueDepthCollector>();
        
        return services;
    }
}
