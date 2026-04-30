using System.Text.Json;
using Domain.Interfaces;

namespace Infrastructure.Messaging;

public class JsonMessageSerializer : ISerializer
{
    private readonly JsonSerializerOptions _options = new()
    {
        // Преобразует поля в CamelCase
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        // Убирает лишние символы: отступы переносы строк и т.п.
        WriteIndented = false,
        // Убираем поля null из json
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    public byte[] Serialize<T>(T value) => 
        System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T Deserialize<T>(byte[] data) => 
        System.Text.Json.JsonSerializer.Deserialize<T>(data, _options) 
        ?? throw new InvalidOperationException($"Failed to deserialize to {typeof(T).Name}");
}
