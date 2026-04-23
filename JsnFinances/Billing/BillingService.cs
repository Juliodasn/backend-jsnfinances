using System.Text.Json;
using JsnFinances.Api.Data;
using JsnFinances.Api.Domain;

namespace JsnFinances.Api.Billing;

public sealed class BillingService
{
    private readonly JsnFinancesDb _db;
    private readonly MercadoPagoClient _mercadoPago;
    private readonly MercadoPagoWebhookValidator _webhookValidator;

    public BillingService(
        JsnFinancesDb db,
        MercadoPagoClient mercadoPago,
        MercadoPagoWebhookValidator webhookValidator)
    {
        _db = db;
        _mercadoPago = mercadoPago;
        _webhookValidator = webhookValidator;
    }

    public Task<IReadOnlyList<BillingPlanDto>> ListPlansAsync()
        => _db.ListBillingPlansAsync();

    public Task<BillingStatusDto> GetStatusAsync(Guid userId)
        => _db.GetOrCreateBillingStatusAsync(userId);

    public async Task<CreateCheckoutResponse> CreateCheckoutAsync(Guid userId, string? userEmail, CreateCheckoutRequest request)
    {
        var plan = await _db.GetBillingPlanAsync(request.PlanCode)
            ?? throw new ArgumentException("Plano não encontrado ou inativo.");

        var payerEmail = !string.IsNullOrWhiteSpace(request.PayerEmail) ? request.PayerEmail : userEmail;
        if (string.IsNullOrWhiteSpace(payerEmail) || !payerEmail.Contains('@'))
        {
            throw new ArgumentException("E-mail do pagador não encontrado. Informe um e-mail válido para o Mercado Pago.");
        }

        if (string.IsNullOrWhiteSpace(request.CardTokenId))
        {
            throw new ArgumentException("Não foi possível gerar o token do cartão. Recarregue a página e tente novamente.");
        }

        var subscription = await _mercadoPago.CreateAuthorizedSubscriptionAsync(
            plan,
            userId,
            payerEmail.Trim(),
            request.CardTokenId.Trim(),
            request.MeliSessionId);

        await _db.SavePendingCheckoutAsync(userId, plan, subscription.Id, subscription.InitPoint, payerEmail.Trim(), subscription.Status);

        return new CreateCheckoutResponse(subscription.InitPoint, subscription.Id, subscription.Status);
    }

    public async Task<BillingStatusDto> SyncUserSubscriptionAsync(Guid userId)
    {
        var providerSubscriptionId = await _db.GetProviderSubscriptionIdAsync(userId);
        if (!string.IsNullOrWhiteSpace(providerSubscriptionId))
        {
            var subscription = await _mercadoPago.GetSubscriptionAsync(providerSubscriptionId);
            await _db.UpdateSubscriptionFromProviderAsync(
                userId,
                subscription.Id,
                subscription.Status,
                null,
                subscription.InitPoint,
                subscription.NextPaymentDate,
                subscription.RawPayload);
        }

        return await _db.GetOrCreateBillingStatusAsync(userId);
    }

    public async Task CancelUserSubscriptionAsync(Guid userId)
    {
        var providerSubscriptionId = await _db.GetProviderSubscriptionIdAsync(userId);
        if (string.IsNullOrWhiteSpace(providerSubscriptionId))
        {
            throw new InvalidOperationException("Nenhuma assinatura do Mercado Pago foi encontrada para este usuário.");
        }

        await _mercadoPago.CancelSubscriptionAsync(providerSubscriptionId);
        var subscription = await _mercadoPago.GetSubscriptionAsync(providerSubscriptionId);
        await _db.UpdateSubscriptionFromProviderAsync(
            userId,
            subscription.Id,
            subscription.Status,
            null,
            subscription.InitPoint,
            subscription.NextPaymentDate,
            subscription.RawPayload);
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
            Guid? paymentUserId = null;

            if (!string.IsNullOrWhiteSpace(payment.ExternalReference) && Guid.TryParse(payment.ExternalReference, out var externalUserId))
            {
                paymentUserId = externalUserId;
            }

            if (paymentUserId is null && !string.IsNullOrWhiteSpace(payment.ProviderSubscriptionId))
            {
                paymentUserId = await _db.GetUserIdByProviderSubscriptionIdAsync(payment.ProviderSubscriptionId);
            }

            if (paymentUserId.HasValue)
            {
                await _db.UpsertPaymentFromProviderAsync(
                    paymentUserId.Value,
                    payment.Id,
                    payment.ProviderSubscriptionId,
                    payment.Status,
                    payment.Amount,
                    payment.Currency,
                    payment.PaidAt,
                    payment.RawPayload);
            }

            return;
        }

        if (eventType.Contains("subscription", StringComparison.OrdinalIgnoreCase)
            || eventType.Contains("preapproval", StringComparison.OrdinalIgnoreCase)
            || (action?.Contains("subscription", StringComparison.OrdinalIgnoreCase) ?? false)
            || (action?.Contains("preapproval", StringComparison.OrdinalIgnoreCase) ?? false))
        {
            var subscription = await _mercadoPago.GetSubscriptionAsync(dataId);
            await _db.UpdateSubscriptionFromWebhookAsync(
                subscription.Id,
                subscription.Status,
                null,
                subscription.InitPoint,
                subscription.NextPaymentDate,
                subscription.RawPayload);
        }
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
