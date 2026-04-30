// Контракт для сбора бизнес- и инфраструктурных метрик.
// Позволяет абстрагироваться от конкретной реализации (Prometheus, OpenTelemetry, etc.)

namespace Application.Interfaces;

public interface IMetricsCollector
{
    /// <summary>
    /// Фиксирует публикацию сообщения в очередь
    /// </summary>
    void RecordMessagePublished(string queueName, string messageType);

    /// <summary>
    /// Фиксирует успешную обработку сообщения
    /// </summary>
    void RecordMessageProcessed(string queueName, string messageType, double processingTimeMs);

    /// <summary>
    /// Фиксирует ошибку при обработке сообщения
    /// </summary>
    void RecordMessageError(string queueName, string messageType, string errorType);

    /// <summary>
    /// Фиксирует размер очереди (опрос брокера)
    /// </summary>
    void RecordQueueDepth(string queueName, int depth);

    /// <summary>
    /// Возвращает текущие метрики для экспорта (Prometheus-совместимый формат)
    /// </summary>
    string ExportToPrometheusFormat();
}
