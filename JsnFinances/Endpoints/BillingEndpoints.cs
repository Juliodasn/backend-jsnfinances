using JsnFinances.Api.Auth;
using JsnFinances.Api.Billing;
using JsnFinances.Api.Domain;

namespace JsnFinances.Api.Endpoints;

public static class BillingEndpoints
{
    public static void MapBillingApi(this WebApplication app)
    {
        var api = app.MapGroup("/api/billing");

        api.MapGet("/plans", async (BillingService billing) =>
            Results.Ok(await billing.ListPlansAsync()));

        api.MapGet("/status", async (HttpContext ctx, IUserContext userContext, BillingService billing) =>
            Results.Ok(await billing.GetStatusAsync(userContext.GetUserId(ctx))));

        api.MapPost("/pix", async (
            HttpContext ctx,
            IUserContext userContext,
            BillingService billing,
            CreatePixChargeRequest request) =>
        {
            var userId = userContext.GetUserId(ctx);
            var email = userContext.GetUserEmail(ctx);
            return Results.Ok(await billing.CreatePixChargeAsync(userId, email, request));
        });

        api.MapGet("/pix/{id:guid}", async (
            HttpContext ctx,
            IUserContext userContext,
            BillingService billing,
            Guid id) =>
        {
            var userId = userContext.GetUserId(ctx);
            return Results.Ok(await billing.GetPixChargeAsync(userId, id));
        });


        api.MapPost("/sync", async (HttpContext ctx, IUserContext userContext, BillingService billing) =>
            Results.Ok(await billing.SyncUserSubscriptionAsync(userContext.GetUserId(ctx))));

        api.MapPost("/cancel", async (HttpContext ctx, IUserContext userContext, BillingService billing) =>
        {
            await billing.CancelUserSubscriptionAsync(userContext.GetUserId(ctx));
            return Results.NoContent();
        });

        api.MapPost("/webhook/mercadopago", async (HttpContext ctx, BillingService billing) =>
        {
            using var reader = new StreamReader(ctx.Request.Body);
            var rawBody = await reader.ReadToEndAsync();
            await billing.ProcessWebhookAsync(ctx, rawBody);
            return Results.Ok(new { received = true });
        });
    }
}
