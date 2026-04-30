namespace Domain.Interfaces;

public interface IMessageProducer
{
    /// <summary>
    /// Публикует сообщение в указанный exchange с заданным routing key
    /// </summary>
    /// <typeparam name="T">Тип сообщения (сериализуется в JSON)</typeparam>
    /// <param name="message">Сообщение для отправки</param>
    /// <param name="exchange">Имя обменника (опционально, по умолчанию из настроек)</param>
    /// <param name="routingKey">Ключ маршрутизации</param>
    /// <param name="cancellationToken">Токен отмены</param>
    Task PublishAsync<T>(
        T message, 
        string routingKey, 
        string? exchange = null,
        CancellationToken cancellationToken = default);
}
