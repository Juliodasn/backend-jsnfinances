using JsnFinances.Api.Admin;
using JsnFinances.Api.Auth;
using JsnFinances.Api.Data;

namespace JsnFinances.Api.Endpoints;

public static class AdminEndpoints
{
    public static void MapAdminApi(this WebApplication app)
    {
        var api = app.MapGroup("/api/admin");

        api.MapGet("/me", async (HttpContext ctx, IUserContext userContext, AdminAccessService admin) =>
            Results.Ok(await admin.RequireAdminAsync(ctx, userContext)));

        api.MapGet("/dashboard", async (HttpContext ctx, IUserContext userContext, AdminAccessService admin, JsnFinancesDb db) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            return Results.Ok(await db.GetAdminDashboardAsync());
        });

        api.MapGet("/usuarios", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            string? search,
            string? status,
            int? page,
            int? pageSize) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            return Results.Ok(await db.ListAdminUsersAsync(search, status, page ?? 1, pageSize ?? 20));
        });

        api.MapGet("/assinaturas", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            string? search,
            string? status,
            int? page,
            int? pageSize) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            return Results.Ok(await db.ListAdminSubscriptionsAsync(search, status, page ?? 1, pageSize ?? 20));
        });

        api.MapGet("/pagamentos", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            string? search,
            string? status,
            int? page,
            int? pageSize) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            return Results.Ok(await db.ListAdminPaymentsAsync(search, status, page ?? 1, pageSize ?? 20));
        });

        api.MapGet("/eventos-billing", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            string? search,
            int? page,
            int? pageSize) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            return Results.Ok(await db.ListAdminBillingEventsAsync(search, page ?? 1, pageSize ?? 20));
        });

        api.MapGet("/logs-acesso", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            int? page,
            int? pageSize) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            return Results.Ok(await db.ListAdminAccessLogsAsync(page ?? 1, pageSize ?? 20));
        });

        api.MapGet("/onboarding-perfis", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            string? search,
            int? page,
            int? pageSize) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            return Results.Ok(await db.ListAdminOnboardingProfilesAsync(search, page ?? 1, pageSize ?? 20));
        });
    }
}
