using Application.Interfaces;
using Domain.Events;

namespace Application.Services;

public class IncomingMessageHandler : IIncomingMessageHandler
{
    public Task<OutputMessage> HandleAsync(InputMessage message, CancellationToken ct)
    {
        // 🔹 Бизнес-правило 1: Валидация
        if (message.Priority == 99)
            throw new InvalidOperationException("Forbidden priority value: 99");

        // 🔹 Бизнес-правило 2: Трансформация
        var processedText = message.Text.ToUpperInvariant();

        return Task.FromResult(new OutputMessage(
            InputMessageId: message.Id,
            ProcessedText: processedText,
            ProcessedAt: DateTime.UtcNow,
            Status: "Success"));
    }
}
