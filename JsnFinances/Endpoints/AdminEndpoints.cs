using JsnFinances.Api.Admin;
using JsnFinances.Api.Auth;
using JsnFinances.Api.Data;
using JsnFinances.Api.Domain;

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



        api.MapGet("/usuarios/{targetUserId:guid}/detalhes", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            Guid targetUserId) =>
        {
            await admin.RequireAdminAsync(ctx, userContext);
            var detail = await db.GetAdminUserDetailsAsync(targetUserId);
            return detail is null ? Results.NotFound(new { message = "Usuário não encontrado." }) : Results.Ok(detail);
        });

        api.MapPost("/usuarios/{targetUserId:guid}/acoes/bloquear", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            Guid targetUserId,
            AdminUserActionRequest request) =>
        {
            var identity = await admin.RequireAdminAsync(ctx, userContext);
            var ok = await db.SetAdminUserBlockAsync(identity.IdUsuario, identity.Email, targetUserId, true, request.Reason);
            if (!ok) return Results.NotFound(new { message = "Usuário não encontrado para bloqueio." });
            return Results.Ok(await db.GetAdminUserDetailsAsync(targetUserId));
        });

        api.MapPost("/usuarios/{targetUserId:guid}/acoes/desbloquear", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            Guid targetUserId,
            AdminUserActionRequest request) =>
        {
            var identity = await admin.RequireAdminAsync(ctx, userContext);
            var ok = await db.SetAdminUserBlockAsync(identity.IdUsuario, identity.Email, targetUserId, false, request.Reason);
            if (!ok) return Results.NotFound(new { message = "Usuário não encontrado para desbloqueio." });
            return Results.Ok(await db.GetAdminUserDetailsAsync(targetUserId));
        });

        api.MapPost("/usuarios/{targetUserId:guid}/acoes/ativar-acesso", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            Guid targetUserId,
            AdminActivateAccessRequest request) =>
        {
            var identity = await admin.RequireAdminAsync(ctx, userContext);
            var ok = await db.ActivateManualAccessAsync(identity.IdUsuario, identity.Email, targetUserId, request.PlanCode, request.Days, request.Reason);
            if (!ok) return Results.BadRequest(new { message = "Não foi possível ativar o acesso. Confira se o plano existe e está ativo." });
            return Results.Ok(await db.GetAdminUserDetailsAsync(targetUserId));
        });

        api.MapPost("/usuarios/{targetUserId:guid}/acoes/estender-trial", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            Guid targetUserId,
            AdminExtendTrialRequest request) =>
        {
            var identity = await admin.RequireAdminAsync(ctx, userContext);
            var ok = await db.ExtendTrialAsync(identity.IdUsuario, identity.Email, targetUserId, request.Days, request.Reason);
            if (!ok) return Results.BadRequest(new { message = "Não foi possível estender o teste grátis." });
            return Results.Ok(await db.GetAdminUserDetailsAsync(targetUserId));
        });

        api.MapPost("/usuarios/{targetUserId:guid}/acoes/alterar-plano", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            Guid targetUserId,
            AdminChangePlanRequest request) =>
        {
            var identity = await admin.RequireAdminAsync(ctx, userContext);
            var ok = await db.ChangeUserPlanAsync(identity.IdUsuario, identity.Email, targetUserId, request.PlanCode, request.Reason);
            if (!ok) return Results.BadRequest(new { message = "Não foi possível alterar o plano. Confira se o plano existe e está ativo." });
            return Results.Ok(await db.GetAdminUserDetailsAsync(targetUserId));
        });

        api.MapPost("/usuarios/{targetUserId:guid}/acoes/cancelar-assinatura", async (
            HttpContext ctx,
            IUserContext userContext,
            AdminAccessService admin,
            JsnFinancesDb db,
            Guid targetUserId,
            AdminUserActionRequest request) =>
        {
            var identity = await admin.RequireAdminAsync(ctx, userContext);
            var ok = await db.CancelAdminUserSubscriptionAsync(identity.IdUsuario, identity.Email, targetUserId, request.Reason);
            if (!ok) return Results.NotFound(new { message = "Usuário sem assinatura para cancelar." });
            return Results.Ok(await db.GetAdminUserDetailsAsync(targetUserId));
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
