// Тесты для бизнес-обработчика входящих сообщений (IncomingMessageHandler).
// Проверяют чистую бизнес-логику: трансформацию данных и валидацию.
// Не требуют RabbitMQ — это юнит-тесты слоя Application.

using Application.Services;
using Domain.Events;
using Xunit;

namespace IntegrationTest;

public class ConsumerTests
{
    private readonly IncomingMessageHandler _handler = new();

    [Fact]
    public async Task HandleAsync_ValidMessage_ShouldReturnSuccessOutput()
    {
        // ARRANGE: Валидное входящее сообщение
        var input = new InputMessage(
            Id: Guid.NewGuid(),
            Text: "hello world",
            Priority: 5,
            Timestamp: DateTime.UtcNow
        );

        // ACT: Обрабатываем сообщение через бизнес-сервис
        var result = await _handler.HandleAsync(input, CancellationToken.None);

        // ASSERT: Проверяем результат обработки
        Assert.Equal(input.Id, result.InputMessageId);
        Assert.Equal("HELLO WORLD", result.ProcessedText); // Бизнес-правило: текст в верхний регистр
        Assert.Equal("Success", result.Status);
    }

    [Fact]
    public async Task HandleAsync_ForbiddenPriority_ShouldThrowException()
    {
        // ARRANGE: Сообщение с запрещённым приоритетом (99)
        var input = new InputMessage(
            Id: Guid.NewGuid(),
            Text: "bad priority",
            Priority: 99, // Триггер бизнес-ошибки
            Timestamp: DateTime.UtcNow
        );

        // ACT & ASSERT: Ожидаем выброс исключения при обработке
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await _handler.HandleAsync(input, CancellationToken.None)
        );
    }
}
