// Тесты сценариев с ошибками: невалидный JSON и нарушения бизнес-правил.
// Проверяют устойчивость сериализатора и обработчика к некорректным данным.

using Application.Services;
using Domain.Events;
using Domain.Interfaces;
using Infrastructure.Messaging;
using System.Text;
using System.Text.Json;
using Xunit;

namespace IntegrationTest;

public class ErrorScenarioTests
{
    [Fact]
    public void Deserialize_InvalidJson_ShouldThrowException()
    {
        // ARRANGE: JSON с невалидным GUID в поле id
        var serializer = new JsonMessageSerializer();
        // Примечание: поля в JSON должны соответствовать настройкам сериализатора (CamelCase)
        var invalidJson = Encoding.UTF8.GetBytes(@"{""id"":""not-a-guid"",""text"":""test"",""priority"":1,""timestamp"":""2024-01-01T00:00:00Z""}");

        // ACT & ASSERT: Проверяем, что десериализация выбрасывает ожидаемое исключение
        var ex = Assert.Throws<JsonException>(() => serializer.Deserialize<InputMessage>(invalidJson));
        
        // Проверяем содержание сообщения об ошибке (реальный текст от System.Text.Json)
        Assert.Contains("id", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("could not be converted", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HandleAsync_BusinessRuleViolation_ShouldThrow()
    {
        // ARRANGE: Обработчик и сообщение с запрещённым приоритетом
        var handler = new IncomingMessageHandler();
        var badInput = new InputMessage(
            Id: Guid.NewGuid(),
            Text: "test",
            Priority: 99, // 🔹 Триггер ошибки (Priority с большой буквы)
            Timestamp: DateTime.UtcNow
        );
        
        // ACT & ASSERT: Ожидаем исключение при нарушении бизнес-правила
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await handler.HandleAsync(badInput, CancellationToken.None)
        );
    }
}
