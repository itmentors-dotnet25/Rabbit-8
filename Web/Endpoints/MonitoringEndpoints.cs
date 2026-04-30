// Эндпоинты для сбора метрик и проверки здоровья.
// Предназначены для Prometheus, Kubernetes liveness/readiness probes, etc.

using Application.Interfaces;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Web.Endpoints;

public static class MonitoringEndpoints
{
    public static IEndpointRouteBuilder MapMonitoringEndpoints(this IEndpointRouteBuilder app)
    {
        // 🔹 Prometheus-style метрики
        app.MapGet("/metrics", (IMetricsCollector metrics) => Results.Text(
                metrics.ExportToPrometheusFormat(), 
                contentType: "text/plain; version=0.0.4"))
        .WithTags("Monitoring")
        .WithOpenApi(op => 
        {
            op.Summary = "Export metrics in Prometheus format";
            op.Description = "Returns application metrics for scraping by Prometheus or manual inspection";
            return op;
        });

        // 🔹 Health check endpoint (интеграция с built-in middleware)
        app.MapHealthChecks("/health", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
        {
            Predicate = check => check.Tags.Contains("live"), // Liveness probe
            ResponseWriter = async (context, report) =>
            {
                context.Response.ContentType = "application/json";
                var result = System.Text.Json.JsonSerializer.Serialize(new
                {
                    status = report.Status.ToString(),
                    checks = report.Entries.Select(e => new
                    {
                        name = e.Key,
                        status = e.Value.Status.ToString(),
                        description = e.Value.Description
                    })
                });
                await context.Response.WriteAsync(result);
            }
        })
        .WithTags("Monitoring");

        // 🔹 Readiness probe (проверяет готовность обрабатывать трафик)
        app.MapHealthChecks("/health/ready", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
        {
            Predicate = check => check.Tags.Contains("ready"),
            ResponseWriter = async (context, report) =>
            {
                context.Response.ContentType = "application/json";
                var result = System.Text.Json.JsonSerializer.Serialize(new
                {
                    status = report.Status.ToString(),
                    ready = report.Status == HealthStatus.Healthy
                });
                await context.Response.WriteAsync(result);
            }
        })
        .WithTags("Monitoring");

        return app;
    }
}
