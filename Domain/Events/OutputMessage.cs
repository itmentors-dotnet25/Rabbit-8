namespace Domain.Events;

public record OutputMessage(
    Guid InputMessageId,  // Связь с исходным сообщением
    string ProcessedText, // Результат обработки
    DateTime ProcessedAt, // Время обработки
    string Status = "Success"); // Статус: Success / Error / Skipped
