namespace Domain.Configuration;

public class RabbitMqSettings
{
    public const string SectionName = "RabbitMQ";

    public string HostName { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string VirtualHost { get; init; } = "/";
    
    // Топология
    public string ExchangeName { get; init; } = "orders-exchange";
    public string ExchangeType { get; init; } = "direct";
    public string InputQueueName { get; init; } = "input-queue";
    public string OutputQueueName { get; init; } = "output-queue";
    public string DlqQueueName { get; init; } = "dlq-queue";

    public string ConnectionString => 
        $"amqp://{UserName}:{Password}@{HostName}:{Port}{VirtualHost}";
}
