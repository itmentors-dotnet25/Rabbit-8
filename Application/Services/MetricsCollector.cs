// Реализация сбора метрик на базе System.Diagnostics.Metrics (.NET 8).
// Автоматически интегрируется с OpenTelemetry, если он подключен в проекте.

using System.Diagnostics;
using System.Diagnostics.Metrics;
using Application.Interfaces;

namespace Application.Services;

public class MetricsCollector : IMetricsCollector, IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _messagesPublished;
    private readonly Counter<long> _messagesProcessed;
    private readonly Counter<long> _messagesErrored;
    private readonly Histogram<double> _processingTime;
    private readonly ObservableGauge<int> _queueDepth;
    
    // Хранилище для экспорта в текстовом формате
    private readonly Dictionary<string, int> _queueDepths = new();
    private readonly object _lock = new();

    public MetricsCollector()
    {
        _meter = new Meter("RabbitMQ.Homework", version: "1.0");
        
        _messagesPublished = _meter.CreateCounter<long>(
            "rabbitmq.messages.published", 
            description: "Number of messages published to queues");
        
        _messagesProcessed = _meter.CreateCounter<long>(
            "rabbitmq.messages.processed", 
            description: "Number of messages successfully processed");
        
        _messagesErrored = _meter.CreateCounter<long>(
            "rabbitmq.messages.errored", 
            description: "Number of messages that failed processing");
        
        _processingTime = _meter.CreateHistogram<double>(
            "rabbitmq.processing.time_ms", 
            unit: "ms",
            description: "Time taken to process a message");
        
        _queueDepth = _meter.CreateObservableGauge(
            "rabbitmq.queue.depth",
            () => GetQueueDepthMeasurements(),
            description: "Current depth of monitored queues");
    }

    public void RecordMessagePublished(string queueName, string messageType)
    {
        var tags = new TagList { { "queue", queueName }, { "type", messageType } };
        _messagesPublished.Add(1, tags);
    }

    public void RecordMessageProcessed(string queueName, string messageType, double processingTimeMs)
    {
        var tags = new TagList { { "queue", queueName }, { "type", messageType } };
        _messagesProcessed.Add(1, tags);
        _processingTime.Record(processingTimeMs, tags);
    }

    public void RecordMessageError(string queueName, string messageType, string errorType)
    {
        var tags = new TagList { { "queue", queueName }, { "type", messageType }, { "error", errorType } };
        _messagesErrored.Add(1, tags);
    }

    public void RecordQueueDepth(string queueName, int depth)
    {
        lock (_lock)
        {
            _queueDepths[queueName] = depth;
        }
    }

    private IEnumerable<Measurement<int>> GetQueueDepthMeasurements()
    {
        lock (_lock)
        {
            return _queueDepths.Select(kvp => 
                new Measurement<int>(kvp.Value, new TagList { { "queue", kvp.Key } }));
        }
    }

    public string ExportToPrometheusFormat()
    {
        // Простая реализация для ручного экспорта (в продакшене лучше использовать OpenTelemetry.Exporter.Prometheus)
        var sb = new System.Text.StringBuilder();
        
        sb.AppendLine("# HELP rabbitmq_messages_published Number of messages published to queues");
        sb.AppendLine("# TYPE rabbitmq_messages_published counter");
        // В реальном проекте здесь нужен доступ к внутренним счётчикам
        
        lock (_lock)
        {
            sb.AppendLine("# HELP rabbitmq_queue_depth Current depth of monitored queues");
            sb.AppendLine("# TYPE rabbitmq_queue_depth gauge");
            foreach (var kvp in _queueDepths)
            {
                sb.AppendLine($"rabbitmq_queue_depth{{queue=\"{kvp.Key}\"}} {kvp.Value}");
            }
        }
        
        return sb.ToString();
    }

    public void Dispose()
    {
        _meter?.Dispose();
    }
}
