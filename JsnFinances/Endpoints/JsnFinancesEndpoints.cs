using JsnFinances.Api.Auth;
using JsnFinances.Api.Billing;
using JsnFinances.Api.Data;
using JsnFinances.Api.Domain;
using JsnFinances.Api.Services;

namespace JsnFinances.Api.Endpoints;

public static class JsnFinancesEndpoints
{
    public static void MapJsnFinancesApi(this WebApplication app)
    {
        var api = app.MapGroup("/api");
        api.AddEndpointFilter<BillingAccessFilter>();

        api.MapGet("/health", () => Results.Ok(new { status = "ok", service = "JsnFinances.Api", date = DateTimeOffset.UtcNow }));


        api.MapGet("/perfil", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.GetPerfilAsync(userContext.GetUserId(ctx))));

        api.MapPut("/perfil", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, PerfilRequest request) =>
            Results.Ok(await db.UpsertPerfilAsync(userContext.GetUserId(ctx), request)));

        api.MapGet("/preferencias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.GetPreferenciasAsync(userContext.GetUserId(ctx))));

        api.MapPut("/preferencias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, PreferenciasUsuarioRequest request) =>
            Results.Ok(await db.UpsertPreferenciasAsync(userContext.GetUserId(ctx), request)));

        api.MapGet("/onboarding/profile", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.GetOnboardingProfileAsync(userContext.GetUserId(ctx))));

        api.MapPut("/onboarding/profile", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, OnboardingProfileRequest request) =>
            Results.Ok(await db.UpsertOnboardingProfileAsync(userContext.GetUserId(ctx), request)));


        api.MapGet("/financeiro/movimentacoes", async (
            HttpContext ctx,
            IUserContext userContext,
            JsnFinancesDb db,
            DateOnly? inicio,
            DateOnly? fim,
            int? limit,
            int? offset) =>
        {
            var userId = userContext.GetUserId(ctx);
            var pageLimit = Math.Clamp(limit ?? 1000, 1, 2000);
            var pageOffset = Math.Max(offset ?? 0, 0);

            var entradas = await db.ListMovimentacoesAsync("entradas", userId, inicio, fim, pageLimit, pageOffset);
            var saidas = await db.ListMovimentacoesAsync("saidas", userId, inicio, fim, pageLimit, pageOffset);
            return Results.Ok(new MovimentacoesResponse(entradas, saidas));
        });

        api.MapGet("/entradas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, DateOnly? inicio, DateOnly? fim, int? limit, int? offset) =>
            Results.Ok(await db.ListMovimentacoesAsync("entradas", userContext.GetUserId(ctx), inicio, fim, Math.Clamp(limit ?? 1000, 1, 2000), Math.Max(offset ?? 0, 0))));


        api.MapGet("/entradas/paginado", async (
            HttpContext ctx,
            IUserContext userContext,
            JsnFinancesDb db,
            DateOnly? inicio,
            DateOnly? fim,
            int? page,
            int? pageSize,
            Guid? idConta) =>
            Results.Ok(await db.ListMovimentacoesPagedAsync("entradas", userContext.GetUserId(ctx), inicio, fim, page ?? 1, pageSize ?? 10, idConta)));

        api.MapGet("/saidas/paginado", async (
            HttpContext ctx,
            IUserContext userContext,
            JsnFinancesDb db,
            DateOnly? inicio,
            DateOnly? fim,
            int? page,
            int? pageSize,
            Guid? idConta) =>
            Results.Ok(await db.ListMovimentacoesPagedAsync("saidas", userContext.GetUserId(ctx), inicio, fim, page ?? 1, pageSize ?? 10, idConta)));

        api.MapGet("/categorias/paginado", async (
            HttpContext ctx,
            IUserContext userContext,
            JsnFinancesDb db,
            int? page,
            int? pageSize,
            string? search,
            string? tipo) =>
            Results.Ok(await db.ListCategoriasPagedAsync(userContext.GetUserId(ctx), page ?? 1, pageSize ?? 5, search, tipo)));

        api.MapGet("/relatorios/transacoes", async (
            HttpContext ctx,
            IUserContext userContext,
            JsnFinancesDb db,
            DateOnly? inicio,
            DateOnly? fim,
            int? page,
            int? pageSize,
            string? tipo,
            string? categoria) =>
            Results.Ok(await db.ListTransacoesPagedAsync(userContext.GetUserId(ctx), inicio, fim, page ?? 1, pageSize ?? 10, tipo, categoria)));

        api.MapGet("/contas/transacoes", async (
            HttpContext ctx,
            IUserContext userContext,
            JsnFinancesDb db,
            DateOnly? inicio,
            DateOnly? fim,
            int? page,
            int? pageSize,
            Guid? idConta) =>
            Results.Ok(await db.ListTransacoesPagedAsync(userContext.GetUserId(ctx), inicio, fim, page ?? 1, pageSize ?? 8, "all", null, idConta)));


        api.MapPost("/entradas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, MovimentoRequest request) =>
        {
            var id = await db.InsertMovimentacaoAsync("entradas", userContext.GetUserId(ctx), request);
            return Results.Created($"/api/entradas/{id}", new { id });
        });

        api.MapPut("/entradas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db, MovimentoRequest request) =>
        {
            await db.UpdateMovimentacaoAsync("entradas", userContext.GetUserId(ctx), id, request);
            return Results.NoContent();
        });

        api.MapDelete("/entradas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
        {
            await db.DeleteMovimentacaoAsync("entradas", userContext.GetUserId(ctx), id);
            return Results.NoContent();
        });

        api.MapGet("/saidas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, DateOnly? inicio, DateOnly? fim, int? limit, int? offset) =>
            Results.Ok(await db.ListMovimentacoesAsync("saidas", userContext.GetUserId(ctx), inicio, fim, Math.Clamp(limit ?? 1000, 1, 2000), Math.Max(offset ?? 0, 0))));

        api.MapPost("/saidas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, MovimentoRequest request) =>
        {
            var id = await db.InsertMovimentacaoAsync("saidas", userContext.GetUserId(ctx), request);
            return Results.Created($"/api/saidas/{id}", new { id });
        });

        api.MapPost("/saidas/parcelamento", async (HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules, ParcelamentoRequest request) =>
        {
            var rows = await rules.CriarSaidaParceladaAsync(userContext.GetUserId(ctx), request);
            return Results.Ok(rows);
        });

        api.MapPut("/saidas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db, MovimentoRequest request) =>
        {
            await db.UpdateMovimentacaoAsync("saidas", userContext.GetUserId(ctx), id, request);
            return Results.NoContent();
        });

        api.MapDelete("/saidas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
        {
            await db.DeleteMovimentacaoAsync("saidas", userContext.GetUserId(ctx), id);
            return Results.NoContent();
        });

        api.MapGet("/categorias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.ListCategoriasAsync(userContext.GetUserId(ctx))));

        api.MapGet("/subcategorias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, Guid? idCategoria) =>
            Results.Ok(await db.ListSubcategoriasAsync(userContext.GetUserId(ctx), idCategoria)));

        api.MapPost("/subcategorias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, SubcategoriaRequest request) =>
        {
            var id = await db.InsertSubcategoriaAsync(userContext.GetUserId(ctx), request);
            return Results.Created($"/api/subcategorias/{id}", new { id });
        });

        api.MapPost("/categorias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, CategoriaRequest request) =>
        {
            var id = await db.InsertCategoriaAsync(userContext.GetUserId(ctx), request);
            return Results.Created($"/api/categorias/{id}", new { id });
        });

        api.MapPut("/categorias/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db, CategoriaRequest request) =>
        {
            await db.UpdateCategoriaAsync(userContext.GetUserId(ctx), id, request);
            return Results.NoContent();
        });

        api.MapDelete("/categorias/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
        {
            await db.DeleteCategoriaAsync(userContext.GetUserId(ctx), id);
            return Results.NoContent();
        });

        api.MapGet("/metas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.ListMetasAsync(userContext.GetUserId(ctx))));

        api.MapPost("/metas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, MetaRequest request) =>
        {
            var id = await db.InsertMetaAsync(userContext.GetUserId(ctx), request);
            return Results.Created($"/api/metas/{id}", new { id });
        });

        api.MapPut("/metas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db, MetaRequest request) =>
        {
            await db.UpdateMetaAsync(userContext.GetUserId(ctx), id, request);
            return Results.NoContent();
        });

        api.MapPost("/metas/{id:guid}/guardar", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db, GuardarValorMetaRequest request) =>
            Results.Ok(await db.AddValueToMetaAsync(userContext.GetUserId(ctx), id, request.Valor)));

        api.MapDelete("/metas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
        {
            await db.DeleteMetaAsync(userContext.GetUserId(ctx), id);
            return Results.NoContent();
        });

        api.MapGet("/orcamentos", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.ListOrcamentosAsync(userContext.GetUserId(ctx))));

        api.MapPost("/orcamentos", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, OrcamentoRequest request) =>
        {
            await db.UpsertOrcamentoAsync(userContext.GetUserId(ctx), request);
            return Results.NoContent();
        });

        api.MapDelete("/orcamentos/{categoria}", async (string categoria, HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
        {
            await db.DeleteOrcamentoAsync(userContext.GetUserId(ctx), categoria);
            return Results.NoContent();
        });

        api.MapGet("/contas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.ListContasAsync(userContext.GetUserId(ctx))));

        api.MapGet("/contas/saldos", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.ListContasSaldoAsync(userContext.GetUserId(ctx))));

        api.MapPost("/contas", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, ContaRequest request) =>
        {
            var id = await db.UpsertContaAsync(userContext.GetUserId(ctx), null, request);
            return Results.Created($"/api/contas/{id}", new { id });
        });

        api.MapPut("/contas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db, ContaRequest request) =>
        {
            await db.UpsertContaAsync(userContext.GetUserId(ctx), id, request);
            return Results.NoContent();
        });

        api.MapDelete("/contas/{id:guid}", async (Guid id, HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
        {
            await db.DeleteContaAsync(userContext.GetUserId(ctx), id);
            return Results.NoContent();
        });

        api.MapGet("/transferencias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db) =>
            Results.Ok(await db.ListTransferenciasAsync(userContext.GetUserId(ctx))));

        api.MapPost("/transferencias", async (HttpContext ctx, IUserContext userContext, JsnFinancesDb db, TransferenciaRequest request) =>
        {
            var id = await db.InsertTransferenciaAsync(userContext.GetUserId(ctx), request);
            return Results.Created($"/api/transferencias/{id}", new { id });
        });

        api.MapGet("/relatorios/resumo", async (DateOnly? dataInicio, DateOnly? dataFim, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarResumoAsync(userContext.GetUserId(ctx), dataInicio, dataFim)));

        api.MapGet("/relatorios/overview", async (DateOnly? dataInicio, DateOnly? dataFim, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarRelatoriosOverviewAsync(userContext.GetUserId(ctx), dataInicio, dataFim)));

        api.MapGet("/insights", async (DateOnly? dataInicio, DateOnly? dataFim, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarInsightsAsync(userContext.GetUserId(ctx), dataInicio, dataFim)));

api.MapPost("/regras/validar-movimento", (JsnFinancesRulesService rules, MovimentoRequest request) =>
            Results.Ok(rules.ValidarMovimento(request)));

        api.MapPost("/regras/parcelamento/preview", (JsnFinancesRulesService rules, ParcelamentoRequest request) =>
            Results.Ok(rules.CriarPreviewParcelamento(request)));

        api.MapGet("/dashboard/resumo", async (DateOnly? dataInicio, DateOnly? dataFim, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarDashboardResumoAsync(userContext.GetUserId(ctx), dataInicio, dataFim)));

        api.MapGet("/dashboard/home", async (DateOnly? dataInicio, DateOnly? dataFim, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarDashboardHomeAsync(userContext.GetUserId(ctx), dataInicio, dataFim)));

        api.MapGet("/orcamentos/uso", async (DateOnly? dataInicio, DateOnly? dataFim, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarUsoOrcamentosAsync(userContext.GetUserId(ctx), dataInicio, dataFim)));

        api.MapGet("/metas/resumo", async (HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarResumoMetasAsync(userContext.GetUserId(ctx))));

api.MapGet("/visao-anual", async (int? ano, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarVisaoAnualAsync(userContext.GetUserId(ctx), ano)));

        api.MapGet("/calendario/movimentacoes", async (int? ano, int? mes, DateOnly? dataInicio, DateOnly? dataFim, HttpContext ctx, IUserContext userContext, JsnFinancesRulesService rules) =>
            Results.Ok(await rules.CriarCalendarioMensalAsync(userContext.GetUserId(ctx), ano, mes, dataInicio, dataFim)));
    }
}
