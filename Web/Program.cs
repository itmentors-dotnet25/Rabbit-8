using Infrastructure;
using Web.Endpoints;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// 🔹 Регистрация RabbitMQ
builder.Services.AddRabbitMqInfrastructure(builder.Configuration);

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
    
    // Регистрация эндпоинтов
    app.MapTestRabbitEndpoints();
}

app.UseHttpsRedirection();

// Мониторинг доступен всегда (включая production)
app.MapMonitoringEndpoints();

app.UseRouting();

app.Run();
