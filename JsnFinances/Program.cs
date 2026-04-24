using JsnFinances.Api.Admin;
using JsnFinances.Api.Auth;
using JsnFinances.Api.Billing;
using JsnFinances.Api.Data;
using JsnFinances.Api.Endpoints;
using JsnFinances.Api.Services;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddHttpContextAccessor();
builder.Services.AddSingleton<IUserContext, SupabaseJwtUserContext>();
builder.Services.AddScoped<JsnFinancesDb>();
builder.Services.AddScoped<JsnFinancesRulesService>();
builder.Services.AddHttpClient<MercadoPagoClient>(client =>
{
    client.BaseAddress = new Uri("https://api.mercadopago.com/");
});
builder.Services.AddScoped<MercadoPagoWebhookValidator>();
builder.Services.AddScoped<BillingService>();
builder.Services.AddScoped<AdminAccessService>();

var connectionString = builder.Configuration.GetConnectionString("SupabasePostgres")
    ?? throw new InvalidOperationException("ConnectionStrings:SupabasePostgres não configurada.");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));

var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins")
    .GetChildren()
    .Select(x => x.Value)
    .Where(x => !string.IsNullOrWhiteSpace(x))
    .Cast<string>()
    .ToArray();
builder.Services.AddCors(options =>
{
    options.AddPolicy("JsnFinancesFront", policy =>
    {
        policy
            .SetIsOriginAllowed(origin =>
                allowedOrigins.Contains(origin, StringComparer.OrdinalIgnoreCase) ||
                origin.StartsWith("http://localhost", StringComparison.OrdinalIgnoreCase) ||
                origin.StartsWith("http://127.0.0.1", StringComparison.OrdinalIgnoreCase))
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseCors("JsnFinancesFront");

app.Use(async (context, next) =>
{
    await next();

    if (context.Response.StatusCode >= 500)
    {
        Console.WriteLine($"HTTP {context.Response.StatusCode} em {context.Request.Method} {context.Request.Path}");
    }
});

app.Use(async (context, next) =>
{
    try
    {
        await next();
    }
    catch (ForbiddenAccessException ex)
    {
        context.Response.StatusCode = StatusCodes.Status403Forbidden;
        await context.Response.WriteAsJsonAsync(new { message = ex.Message });
    }
    catch (UnauthorizedAccessException ex)
    {
        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        await context.Response.WriteAsJsonAsync(new { message = ex.Message });
    }
    catch (ArgumentException ex)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        await context.Response.WriteAsJsonAsync(new { message = ex.Message });
    }
    catch (InvalidOperationException ex)
    {
        context.Response.StatusCode = StatusCodes.Status409Conflict;
        await context.Response.WriteAsJsonAsync(new { message = ex.Message });
    }
    catch (Exception ex)
    {
        Console.WriteLine("=== EXCEPTION START ===");
        Console.WriteLine(ex.ToString());
        Console.WriteLine("=== EXCEPTION END ===");

        context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        await context.Response.WriteAsJsonAsync(new
        {
            message = "Erro inesperado na API JSN Finances.",
            detail = app.Environment.IsDevelopment() ? ex.Message : null
        });
    }
});

app.MapBillingApi();
app.MapAdminApi();
app.MapJsnFinancesApi();
app.Run();
