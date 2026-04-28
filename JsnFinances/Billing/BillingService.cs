using System.Text.Json;
using JsnFinances.Api.Data;
using JsnFinances.Api.Domain;
using Microsoft.Extensions.Caching.Memory;

namespace JsnFinances.Api.Billing;

public sealed class BillingService
{
    private readonly JsnFinancesDb _db;
    private readonly MercadoPagoClient _mercadoPago;
    private readonly MercadoPagoWebhookValidator _webhookValidator;
    private readonly IMemoryCache _cache;

    public BillingService(
        JsnFinancesDb db,
        MercadoPagoClient mercadoPago,
        MercadoPagoWebhookValidator webhookValidator,
        IMemoryCache cache)
    {
        _db = db;
        _mercadoPago = mercadoPago;
        _webhookValidator = webhookValidator;
        _cache = cache;
    }

    public Task<IReadOnlyList<BillingPlanDto>> ListPlansAsync()
        => _db.ListBillingPlansAsync();

    public async Task<BillingStatusDto> GetStatusAsync(Guid userId, bool forceRefresh = false)
    {
        var cacheKey = GetBillingStatusCacheKey(userId);
        if (!forceRefresh && _cache.TryGetValue(cacheKey, out BillingStatusDto? cached) && cached is not null)
        {
            return cached;
        }

        var status = await _db.GetOrCreateBillingStatusAsync(userId);
        _cache.Set(cacheKey, status, TimeSpan.FromSeconds(30));
        return status;
    }

    public async Task<PixChargeDto> CreatePixChargeAsync(Guid userId, string? userEmail, CreatePixChargeRequest request)
    {
        var planCode = NormalizePlanCode(request.PlanCode);
        var plan = await _db.GetBillingPlanAsync(planCode)
            ?? throw new ArgumentException("Plano não encontrado ou inativo. Use mensal ou anual.");

        var payerEmail = !string.IsNullOrWhiteSpace(request.PayerEmail) ? request.PayerEmail : userEmail;
        if (string.IsNullOrWhiteSpace(payerEmail) || !payerEmail.Contains('@'))
        {
            throw new ArgumentException("E-mail do pagador não encontrado. Informe um e-mail válido para gerar o PIX.");
        }

        var chargeId = Guid.NewGuid();
        var pix = await _mercadoPago.CreatePixPaymentAsync(plan, userId, chargeId, payerEmail.Trim());

        return await _db.SavePixChargeAsync(userId, plan, chargeId, pix);
    }

    public async Task<PixChargeDto> GetPixChargeAsync(Guid userId, Guid chargeId)
    {
        var charge = await _db.GetPixChargeAsync(userId, chargeId)
            ?? throw new KeyNotFoundException("Cobrança PIX não encontrada para este usuário.");

        if (charge.PaymentStatus is "pending" or "in_process" && !string.IsNullOrWhiteSpace(charge.MercadoPagoPaymentId))
        {
            var payment = await _mercadoPago.GetPaymentAsync(charge.MercadoPagoPaymentId);
            charge = await _db.UpdatePixChargeFromPaymentAsync(userId, charge.Id, payment);
        }

        return charge;
    }

    public async Task<BillingStatusDto> SyncUserSubscriptionAsync(Guid userId)
    {
        _cache.Remove(GetBillingStatusCacheKey(userId));
        return await GetStatusAsync(userId, forceRefresh: true);
    }

    public async Task CancelUserSubscriptionAsync(Guid userId)
    {
        await _db.CancelUserAccessAsync(userId);
        _cache.Remove(GetBillingStatusCacheKey(userId));
    }

    public async Task ProcessWebhookAsync(HttpContext context, string rawBody)
    {
        using var document = JsonDocument.Parse(string.IsNullOrWhiteSpace(rawBody) ? "{}" : rawBody);
        var root = document.RootElement;

        var eventType = GetString(root, "type")
            ?? GetString(root, "topic")
            ?? context.Request.Query["type"].FirstOrDefault()
            ?? context.Request.Query["topic"].FirstOrDefault()
            ?? "unknown";
        var action = GetString(root, "action") ?? context.Request.Query["action"].FirstOrDefault();
        var dataId = context.Request.Query["data.id"].FirstOrDefault()
            ?? context.Request.Query["id"].FirstOrDefault()
            ?? ReadNestedDataId(root);
        var requestId = context.Request.Headers["x-request-id"].FirstOrDefault();
        var signatureValid = _webhookValidator.Validate(context, dataId);

        if (_webhookValidator.IsSignatureRequired && !signatureValid)
        {
            await _db.InsertBillingEventAsync(eventType, action, dataId, requestId, false, rawBody);
            throw new UnauthorizedAccessException("Webhook Mercado Pago inválido: assinatura não conferiu.");
        }

        await _db.InsertBillingEventAsync(eventType, action, dataId, requestId, signatureValid, rawBody);

        if (string.IsNullOrWhiteSpace(dataId)) return;

        if (eventType.Contains("payment", StringComparison.OrdinalIgnoreCase)
            || (action?.Contains("payment", StringComparison.OrdinalIgnoreCase) ?? false))
        {
            var payment = await _mercadoPago.GetPaymentAsync(dataId);
            await _db.UpdatePixChargeFromWebhookPaymentAsync(payment);
        }
    }

    private static string GetBillingStatusCacheKey(Guid userId)
        => $"billing-status:{userId:N}";

    private static string NormalizePlanCode(string? planCode)
    {
        var normalized = (planCode ?? string.Empty).Trim().ToLowerInvariant();
        if (normalized is not ("mensal" or "anual"))
        {
            throw new ArgumentException("Plano inválido. Os valores permitidos são mensal e anual.");
        }
        return normalized;
    }

    private static string? ReadNestedDataId(JsonElement root)
    {
        if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Object) return null;
        return GetString(data, "id");
    }

    private static string? GetString(JsonElement root, string propertyName)
        => root.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null
            ? property.ToString()
            : null;
}
