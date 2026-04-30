using Domain.Events;

namespace Application.Interfaces;

/// <summary>
/// Обрабатывает входящее сообщение из input-queue (бизнес-логика)
/// </summary>
public interface IIncomingMessageHandler
{
    Task<OutputMessage> HandleAsync(InputMessage message, CancellationToken ct);
}
