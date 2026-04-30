// Проверка здоровья подключения к RabbitMQ.
// Интегрируется с ASP.NET Core Health Checks middleware.

using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;

namespace Infrastructure.Monitoring;

public class RabbitMqHealthCheck(IConnection connection) : IHealthCheck
{
    private readonly IConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Проверяем, что соединение активно
            if (_connection.IsOpen)
            {
                // Дополнительно: пробуем создать временный канал для проверки прав
                using var channel = _connection.CreateModel();
                return Task.FromResult(HealthCheckResult.Healthy("RabbitMQ connection is active"));
            }
            
            return Task.FromResult(HealthCheckResult.Degraded("RabbitMQ connection is not open"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("RabbitMQ health check failed", ex));
        }
    }
}
