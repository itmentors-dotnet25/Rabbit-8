namespace Domain.Configuration;

public class PollySettings
{
    public const string SectionName = "Polly";

    /// <summary>
    /// Настройки политики повторных попыток
    /// </summary>
    public RetrySettings Retry { get; init; } = new();

    /// <summary>
    /// Вложенный класс для Retry-политики
    /// </summary>
    public class RetrySettings
    {
        /// <summary>
        /// Включена ли политика повторов
        /// </summary>
        public bool Enabled { get; init; } = true;

        /// <summary>
        /// Максимальное количество повторных попыток
        /// </summary>
        public int RetryCount { get; init; } = 3;

        /// <summary>
        /// Базовая задержка перед первой попыткой (в секундах)
        /// </summary>
        public int InitialDelaySeconds { get; init; } = 2;

        /// <summary>
        /// Использовать экспоненциальное увеличение задержки: 2s → 4s → 8s
        /// </summary>
        public bool UseExponentialBackoff { get; init; } = true;
    }
}
