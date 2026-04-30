// Фоновая задача для периодического опроса размеров очередей в RabbitMQ.
// Запускается как IHostedService, обновляет метрики каждые 30 секунд.

using Application.Interfaces;
using Domain.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Infrastructure.Monitoring;

public class QueueDepthCollector : BackgroundService
{
    private readonly IConnection _connection;
    private readonly RabbitMqSettings _settings;
    private readonly IMetricsCollector _metrics;
    private readonly ILogger<QueueDepthCollector> _logger;
    private readonly TimeSpan _interval = TimeSpan.FromSeconds(30);

    public QueueDepthCollector(
        IConnection connection,
        RabbitMqSettings settings,
        IMetricsCollector metrics,
        ILogger<QueueDepthCollector> logger)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("📊 QueueDepthCollector started. Polling every {Interval}s", _interval.TotalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var channel = _connection.CreateModel();
                
                // Опрашиваем все известные очереди
                var queues = new[] 
                { 
                    _settings.InputQueueName, 
                    _settings.OutputQueueName, 
                    _settings.DlqQueueName 
                };

                foreach (var queueName in queues)
                {
                    try
                    {
                        var info = channel.QueueDeclarePassive(queueName);
                        _metrics.RecordQueueDepth(queueName, (int)info.MessageCount);
                    }
                    catch (RabbitMQ.Client.Exceptions.OperationInterruptedException)
                    {
                        // Очередь не существует — игнорируем
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Failed to collect queue depths");
            }

            await Task.Delay(_interval, stoppingToken);
        }

        _logger.LogInformation("🛑 QueueDepthCollector stopped");
    }
}
