namespace Domain.Events;

public record InputMessage(
    Guid Id, 
    string Text, 
    int Priority, 
    DateTime Timestamp);
