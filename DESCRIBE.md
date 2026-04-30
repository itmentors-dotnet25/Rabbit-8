



📘 DESCRIBE.md: RabbitMQ Integration (.NET 8)

🎯 Обзор проекта

Чистая, производственная реализация асинхронной коммуникации через RabbitMQ на стеке .NET 8 + Clean Architecture. Проект демонстрирует полный цикл работы с брокером сообщений: от типобезопасной публикации и фоновой обработки до отказоустойчивости, изоляции ошибок (DLQ), интеграционных тестов и встроенной телеметрии.


🔹 Ключевые технологии:
RabbitMQ.Client v6.8.1, Polly v8, System.Diagnostics.Metrics, Testcontainers, Minimal API, xUnit.



🏗️ Архитектура решения

Проект разделён по принципам Clean Architecture с чётким разделением ответственности:


┌──────────────────────────────────────────────────┐
│                   Presentation                   │
│            (Minimal API / Swagger)               │
└────────────────────┬─────────────────────────────┘
│
┌────────────────────▼─────────────────────────────┐
│                  Application                     │
│            (Business Logic, DTOs)                │
└────────────────────┬─────────────────────────────┘
│
┌────────────────────▼─────────────────────────────┐
│                  Infrastructure                  │
│     RabbitMQ Client, Polly, Serialization, DLQ   │
└──────────────────────────────────────────────────┘


🔄 Поток сообщений

sequenceDiagram
participant P as Producer
participant Q as Input Queue
participant C as Consumer (Input)
participant BL as Business Logic
participant OQ as Output Queue
participant OC as Consumer (Output)

    P->>Q: Publish(message)
    Q->>C: Deliver
    C->>BL: Process(message)
    alt Success
        BL->>C: Result
        C->>OQ: Publish(result)
        OQ->>OC: Deliver
    else Failure
        C->>Q: BasicNack(requeue=false)
        Q->>DLQ: Route to Dead Letter Queue
    end


🛠️ Пошаговая реализация

📦 Шаг 1: Настройка инфраструктуры

Подключаем брокер и инфраструктурные зависимости. Выбираем RabbitMQ.Client v6.8.1, так как в v7+ удалён синхронный CreateConnection(), а мы используем синхронный канал в фоновых задачах.


dotnet add package RabbitMQ.Client --version 6.8.1
dotnet add package Polly --version 8.*
dotnet add package Testcontainers.RabbitMq

🔌 Шаг 2: Сериализация и Продюсер (Producer)


JsonMessageSerializer: Stateless-обёртка над System.Text.Json с CamelCase-политикой.

RabbitMqProducer: Transient-компонент. Лениво создаёт IModel, публикует с mandatory: true (возврат при отсутствии маршрута), логирует действия.

DI: services.AddTransient<IMessageProducer, RabbitMqProducer>()


📡 Шаг 3: Фоновые потребители (Consumers)

Оба потребителя наследуют BackgroundService и живут весь цикл IHost:



InputQueueConsumer: Слушает input-queue, десериализует, делегирует бизнес-логике в Application, публикует результат в output-queue.

OutputQueueConsumer: Финальный обработчик результата (логирование, аналитика, уведомление внешних систем).

DI: services.AddHostedService<...>()


🛡️ Шаг 4: Отказоустойчивость через Polly v8

Интегрируем современную политику повторных попыток. В v8 API изменён: вместо IAsyncPolicy используется ResiliencePipeline.


// Пример конфигурации
var retryPipeline = new ResiliencePipelineBuilder()
.AddRetry(new RetryStrategyOptions
{
MaxRetryAttempts = 3,
Delay = TimeSpan.FromSeconds(2),
BackoffType = DelayBackoffType.Exponential
})
.Build();

Конфигурация вынесена в appsettings.json (Polly.Retry), поддержка экспоненциальной задержки 2s → 4s → 8s. Политика оборачивает вызов бизнес-обработчика внутри InputQueueConsumer.


🗑️ Шаг 5: Механизм Dead Letter Queue (DLQ)

Очередь не умеет "умирать" сама — это паттерн, реализуемый через аргументы декларирования:


channel.QueueDeclare("input-queue", durable: true, exclusive: false, autoDelete: false,
arguments: new Dictionary<string, object>
{
{ "x-dead-letter-exchange", "" },
{ "x-dead-letter-routing-key", "dlq-queue" }
});

🔥 Триггер: BasicNack(deliveryTag, multiple: false, requeue: false). Брокер автоматически перенаправляет сообщение в dlq-queue.


Тестовый сценарий:

Публикуем сообщение с Priority == 99 (нарушение бизнес-правила).

Consumer вызывает BasicNack.

Сообщение попадает в DLQ.


🧪 Шаг 6: Интеграционное тестирование

Поднимаем реальный RabbitMQ в Docker прямо во время тестов.


dotnet add package Testcontainers.RabbitMq
dotnet add package xunit

Файл	Что проверяет	Кол-во тестов
ProducerTests.cs	Публикация, сериализация ↔ десериализация	2
ConsumerTests.cs	Бизнес-логика, валидация Priority == 99	2
ErrorScenarioTests.cs	Некорректный JSON, нарушение бизнес-правил	2
DlqTests.cs	Маршрутизация Nack(requeue:false) → DLQ	1
Итого	Полное покрытие потока и ошибок	7 ✅

📊 Шаг 7: Мониторинг и Health Checks

Используем встроенный в .NET 8 System.Diagnostics.Metrics + стандартные Health Checks.


dotnet add package Microsoft.Extensions.Diagnostics.HealthChecks


IMetricsCollector: Счётчики публикации/ошибок, гистограммы времени обработки, ObservableGauge глубины очередей.

QueueDepthCollector: Фоновая задача, опрашивает брокер каждые 30 сек.

Endpoints: /metrics (Prometheus-формат), /health (Liveness), /health/ready (Readiness).



🔍 Технические детали и архитектурные решения

🔤 Почему RabbitMqProducer vs RabbitMqConsumerService?

Это не случайность, а осознанное разделение ответственности:


Критерий	RabbitMqProducer	InputQueueConsumer
Роль	Утилита отправки	Долгоживущий фоновый процесс
Жизненный цикл	Transient (по запросу)	Singleton (AddHostedService)
Инициализация	Ленивая (при вызове)	Автоматическая при старте хоста
Завершение	GC / IDisposable	CancellationToken + Graceful Shutdown

🏗️ Разбор BackgroundService

public abstract class BackgroundService : IHostedService, IDisposable
{
protected abstract Task ExecuteAsync(CancellationToken stoppingToken);

    public virtual Task StartAsync(CancellationToken cancellationToken)
    {
        // Запускает фоновую задачу
    }

    public virtual Task StopAsync(CancellationToken cancellationToken)
    {
        // Graceful shutdown
    }
}

💡 Почему autoAck: false?

Подтверждение отправляется после успешной обработки. Если сервис упадёт до BasicAck — сообщение вернётся в очередь или уйдёт в DLQ. Это гарантия At-Least-Once доставки.


🔄 BasicAck vs BasicNack

| Метод     | multiple | requeue | Поведение                                     |
|-----------|----------|---------|-----------------------------------------------|
| BasicAck  | false    | –       | Сообщение удалено из очереди                  |
| BasicNack | false    | true    | Вернётся в конец очереди (риск шторма)        |
| BasicNack | false    | false   | ❌ Отказано. При наличии x-dead-letter-* → DLQ |


🔐 Потокобезопасность

IConnection ✅ потокобезопасен (создаётся один раз, живёт всё время приложения).

IModel ❌ не потокобезопасен. В нашей реализации создаётся один раз на ExecuteAsync, а BasicAck/Nack — атомарные операции. Конфликтов нет.



📋 Принципы тестирования

Принцип	Реализация	Зачем
🔒 Изоляция	uniqueId = Guid.NewGuid().ToString("N")[..8]	Исключает конфликты очередей при параллельном запуске
🧼 Чистота	PurgeQueue() + уникальные имена	Гарантирует детерминированное начальное состояние
🎯 Явные ассерты	Проверка реального текста ошибок, а не догадок	Устойчивость к изменениям в System.Text.Json
🐳 Реальный брокер	Testcontainers.RabbitMq	Тесты проверяют поведение RabbitMQ, а не моки


🚀 Запуск и проверка

📦 Предварительные требования


✅ .NET 8 SDK

✅ Docker Desktop (для Testcontainers и локального брокера)


▶️ Запуск приложения

dotnet run --project src/Presentation

🌐 Доступные эндпоинты

URL	Назначение
http://localhost:5125/swagger	Документация API
POST /test/rabbit/publish	Тестовая публикация (отладка DLQ)
GET /test/rabbit/dlq	Просмотр мёртвых сообщений (Swagger)
GET /metrics	Метрики для Prometheus
GET /health	Liveness probe
GET /health/ready	Readiness probe

🧪 Запуск интеграционных тестов

dotnet test

✅ Ожидаемый результат:


Passed! - Failed: 0, Passed: 7, Skipped: 0


📝 Итог

Проект покрывает полный производственный цикл работы с RabbitMQ в экосистеме .NET:



✅ Чистая архитектура с разделением Infrastructure / Application

✅ Отказоустойчивость через Polly v8 + экспоненциальная задержка

✅ Паттерн Dead Letter Queue для изоляции "битых" сообщений

✅ Детерминированные интеграционные тесты с Testcontainers

✅ Встроенная телеметрия и Health Checks для оркестраторов


Готов к масштабированию и подключению к внешним системам мониторинга (Prometheus/Grafana, Jaeger, Seq).
