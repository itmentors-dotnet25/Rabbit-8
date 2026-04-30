// Базовый класс для интеграционных тестов с RabbitMQ через Testcontainers.
// Поднимает изолированный контейнер для каждого тестового класса.
// Все тесты наследуются от этого класса для повторного использования инфраструктуры.

using DotNet.Testcontainers.Builders;
using RabbitMQ.Client;

namespace IntegrationTest;

public abstract class RabbitMqTestBase : IAsyncLifetime
{
    // Используем полное имя, чтобы избежать конфликта с System.ComponentModel.IContainer
    // Testcontainers v4: передаём образ в конструктор билдера
    private readonly DotNet.Testcontainers.Containers.IContainer _container = new ContainerBuilder("rabbitmq:3-management")
        .WithPortBinding(5672, true) // Динамический порт для изоляции тестов
        .WithWaitStrategy(Wait.ForUnixContainer()
            .UntilMessageIsLogged("Server startup complete")) // Ждём готовности брокера
        .Build();
    
    protected IConnection? Connection { get; private set; }
    protected int Port { get; private set; }
    protected const string Host = "localhost";
    protected const string User = "guest";
    protected const string Pass = "guest";

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        Port = _container.GetMappedPublicPort(5672);

        var factory = new ConnectionFactory
        {
            HostName = Host,
            Port = Port,
            UserName = User,
            Password = Pass,
            AutomaticRecoveryEnabled = true
        };
        Connection = factory.CreateConnection();
    }

    public async Task DisposeAsync()
    {
        Connection?.Close();
        Connection?.Dispose();
        await _container.DisposeAsync();
    }

    /// <summary>
    /// Очищает очередь от всех сообщений. Используется для изоляции тестов.
    /// </summary>
    protected void PurgeQueue(string queueName)
    {
        using var channel = Connection!.CreateModel();
        try { channel.QueuePurge(queueName); } catch { /* Игнорируем, если очередь не существует */ }
    }
}
