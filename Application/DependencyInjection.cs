using Application.Interfaces;
using Application.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Application;

public static class DependencyInjection
{
    /// <summary>
    /// Регистрирует сервисы слоя Application
    /// </summary>
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        // Регистрация обработчика входящих сообщений
        services.AddSingleton<IIncomingMessageHandler, IncomingMessageHandler>();
        
        // Регистрация сборщика метрик (синглтон, т.к. метрики глобальны)
        services.AddSingleton<IMetricsCollector, MetricsCollector>();
        
        return services;
    }
}
