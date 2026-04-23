using JsnFinances.Api.Auth;
using Microsoft.AspNetCore.Http.HttpResults;

namespace JsnFinances.Api.Billing;

public sealed class BillingAccessFilter : IEndpointFilter
{
    private static readonly string[] FreePrefixes =
    [
        "/api/health",
        "/api/perfil",
        "/api/preferencias",
        "/api/billing",
        "/api/plans"
    ];

    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var httpContext = context.HttpContext;
        var path = httpContext.Request.Path.Value ?? string.Empty;

        if (FreePrefixes.Any(prefix => path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
        {
            return await next(context);
        }

        var userContext = httpContext.RequestServices.GetRequiredService<IUserContext>();
        var billingService = httpContext.RequestServices.GetRequiredService<BillingService>();
        var userId = userContext.GetUserId(httpContext);
        var status = await billingService.GetStatusAsync(userId);

        if (status.HasAccess)
        {
            return await next(context);
        }

        return Results.Json(new
        {
            message = status.Message,
            billing = status
        }, statusCode: StatusCodes.Status402PaymentRequired);
    }
}
