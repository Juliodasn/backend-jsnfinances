using System.Text.Json;
using JsnFinances.Api.Domain;
using Npgsql;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    public async Task<IReadOnlyList<BillingPlanDto>> ListBillingPlansAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select code, nome, descricao, valor, moeda, frequency, frequency_type, destaque
            from public.billing_plans
            where ativo = true
            order by ordem asc, valor asc
            """;

        var rows = new List<BillingPlanDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new BillingPlanDto(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetDecimal(3),
                reader.GetString(4),
                reader.GetInt32(5),
                reader.GetString(6),
                reader.GetBoolean(7)));
        }

        return rows;
    }

    public async Task<BillingPlanDto?> GetBillingPlanAsync(string planCode)
    {
        if (string.IsNullOrWhiteSpace(planCode)) return null;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select code, nome, descricao, valor, moeda, frequency, frequency_type, destaque
            from public.billing_plans
            where ativo = true and lower(code) = lower(@code)
            limit 1
            """;
        Add(command, "code", planCode.Trim());

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        return new BillingPlanDto(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetDecimal(3),
            reader.GetString(4),
            reader.GetInt32(5),
            reader.GetString(6),
            reader.GetBoolean(7));
    }

    public async Task<BillingStatusDto> GetOrCreateBillingStatusAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();

        await using (var insert = connection.CreateCommand())
        {
            insert.CommandText = """
                insert into public.user_subscriptions
                  (id_usuario, status, trial_started_at, trial_ends_at, provider)
                values
                  (@userId, 'trialing', timezone('utc', now()), timezone('utc', now()) + interval '3 days', 'mercadopago')
                on conflict (id_usuario) do nothing
                """;
            Add(insert, "userId", userId);
            await insert.ExecuteNonQueryAsync();
        }

        var current = await ReadBillingStatusAsync(connection, userId);
        if (current.Status == "trialing" && current.TrialEndsAt.HasValue && DateTimeOffset.UtcNow > current.TrialEndsAt.Value)
        {
            await using var expire = connection.CreateCommand();
            expire.CommandText = """
                update public.user_subscriptions
                set status = 'expired', atualizado_em = timezone('utc', now())
                where id_usuario = @userId
                  and status = 'trialing'
                  and trial_ends_at < timezone('utc', now())
                """;
            Add(expire, "userId", userId);
            await expire.ExecuteNonQueryAsync();
            current = await ReadBillingStatusAsync(connection, userId);
        }

        return current;
    }

    public async Task SavePendingCheckoutAsync(
        Guid userId,
        BillingPlanDto plan,
        string providerSubscriptionId,
        string? initPoint,
        string? payerEmail,
        string? providerStatus = null)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.user_subscriptions
              (id_usuario, status, trial_started_at, trial_ends_at, plan_code, provider,
               provider_subscription_id, provider_payer_email, init_point, last_synced_at)
            values
              (@userId, @status, timezone('utc', now()), timezone('utc', now()) + interval '3 days', @planCode, 'mercadopago',
               @providerSubscriptionId, @payerEmail, @initPoint, timezone('utc', now()))
            on conflict (id_usuario)
            do update set
              status = @status,
              plan_code = excluded.plan_code,
              provider = 'mercadopago',
              provider_subscription_id = excluded.provider_subscription_id,
              provider_payer_email = excluded.provider_payer_email,
              init_point = excluded.init_point,
              last_synced_at = timezone('utc', now()),
              atualizado_em = timezone('utc', now())
            """;
        var normalizedStatus = NormalizeProviderStatus(providerStatus);

        Add(command, "userId", userId);
        Add(command, "status", normalizedStatus);
        Add(command, "planCode", plan.Code);
        Add(command, "providerSubscriptionId", providerSubscriptionId);
        Add(command, "payerEmail", payerEmail);
        Add(command, "initPoint", initPoint);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<string?> GetProviderSubscriptionIdAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select provider_subscription_id
            from public.user_subscriptions
            where id_usuario = @userId
            limit 1
            """;
        Add(command, "userId", userId);

        var value = await command.ExecuteScalarAsync();
        return value is null or DBNull ? null : Convert.ToString(value);
    }

    public async Task<Guid?> GetUserIdByProviderSubscriptionIdAsync(string providerSubscriptionId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id_usuario
            from public.user_subscriptions
            where provider_subscription_id = @providerSubscriptionId
            limit 1
            """;
        Add(command, "providerSubscriptionId", providerSubscriptionId);

        var value = await command.ExecuteScalarAsync();
        return value is Guid id ? id : null;
    }

    public async Task UpdateSubscriptionFromProviderAsync(
        Guid userId,
        string providerSubscriptionId,
        string providerStatus,
        string? planCode,
        string? initPoint,
        DateTimeOffset? nextPaymentDate,
        string rawPayload)
    {
        var normalizedStatus = NormalizeProviderStatus(providerStatus);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            update public.user_subscriptions
            set status = @status,
                plan_code = coalesce(@planCode, plan_code),
                provider = 'mercadopago',
                provider_subscription_id = @providerSubscriptionId,
                init_point = coalesce(@initPoint, init_point),
                current_period_end = @currentPeriodEnd,
                provider_raw_payload = cast(@rawPayload as jsonb),
                last_synced_at = timezone('utc', now()),
                atualizado_em = timezone('utc', now())
            where id_usuario = @userId
            """;
        Add(command, "userId", userId);
        Add(command, "status", normalizedStatus);
        Add(command, "planCode", planCode);
        Add(command, "providerSubscriptionId", providerSubscriptionId);
        Add(command, "initPoint", initPoint);
        Add(command, "currentPeriodEnd", nextPaymentDate);
        Add(command, "rawPayload", string.IsNullOrWhiteSpace(rawPayload) ? "{}" : rawPayload);
        await command.ExecuteNonQueryAsync();
    }

    public async Task UpdateSubscriptionFromWebhookAsync(
        string providerSubscriptionId,
        string providerStatus,
        string? planCode,
        string? initPoint,
        DateTimeOffset? nextPaymentDate,
        string rawPayload)
    {
        var normalizedStatus = NormalizeProviderStatus(providerStatus);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            update public.user_subscriptions
            set status = @status,
                plan_code = coalesce(@planCode, plan_code),
                init_point = coalesce(@initPoint, init_point),
                current_period_end = @currentPeriodEnd,
                provider_raw_payload = cast(@rawPayload as jsonb),
                last_synced_at = timezone('utc', now()),
                atualizado_em = timezone('utc', now())
            where provider_subscription_id = @providerSubscriptionId
            """;
        Add(command, "providerSubscriptionId", providerSubscriptionId);
        Add(command, "status", normalizedStatus);
        Add(command, "planCode", planCode);
        Add(command, "initPoint", initPoint);
        Add(command, "currentPeriodEnd", nextPaymentDate);
        Add(command, "rawPayload", string.IsNullOrWhiteSpace(rawPayload) ? "{}" : rawPayload);
        await command.ExecuteNonQueryAsync();
    }

    public async Task InsertBillingEventAsync(
        string eventType,
        string? action,
        string? resourceId,
        string? requestId,
        bool signatureValid,
        string payload)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.billing_events
              (provider, event_type, event_action, resource_id, request_id, signature_valid, payload)
            values
              ('mercadopago', @eventType, @action, @resourceId, @requestId, @signatureValid, cast(@payload as jsonb))
            """;
        Add(command, "eventType", eventType);
        Add(command, "action", action);
        Add(command, "resourceId", resourceId);
        Add(command, "requestId", requestId);
        Add(command, "signatureValid", signatureValid);
        Add(command, "payload", string.IsNullOrWhiteSpace(payload) ? "{}" : payload);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<BillingStatusDto> ReadBillingStatusAsync(NpgsqlConnection connection, Guid userId)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select status, trial_started_at, trial_ends_at, current_period_end, plan_code, provider_subscription_id, init_point
            from public.user_subscriptions
            where id_usuario = @userId
            limit 1
            """;
        Add(command, "userId", userId);

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return new BillingStatusDto("trialing", true, true, null, null, null, 3, null, null, null, "Teste gratuito iniciado.");
        }
        var status = reader.GetString(0);
        DateTimeOffset? trialStartedAt = reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1);
        DateTimeOffset? trialEndsAt = reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2);
        DateTimeOffset? currentPeriodEnd = reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3);
        var planCode = reader.IsDBNull(4) ? null : reader.GetString(4);
        var providerSubscriptionId = reader.IsDBNull(5) ? null : reader.GetString(5);
        var initPoint = reader.IsDBNull(6) ? null : reader.GetString(6);

        return BuildBillingStatus(status, trialStartedAt, trialEndsAt, currentPeriodEnd, planCode, providerSubscriptionId, initPoint);
    }

    private static BillingStatusDto BuildBillingStatus(
        string status,
        DateTimeOffset? trialStartedAt,
        DateTimeOffset? trialEndsAt,
        DateTimeOffset? currentPeriodEnd,
        string? planCode,
        string? providerSubscriptionId,
        string? initPoint)
    {
        var normalized = NormalizeProviderStatus(status);
        var now = DateTimeOffset.UtcNow;
        var active = normalized == "active";
        var trialEnd = trialEndsAt.GetValueOrDefault();
        var periodEnd = currentPeriodEnd.GetValueOrDefault();
        var trialStillValid = trialEndsAt.HasValue && now <= trialEnd;
        var activeStillValid = active && (!currentPeriodEnd.HasValue || now <= periodEnd);
        var isTrial = !active && trialStillValid;
        var hasAccess = activeStillValid || trialStillValid;
        var daysLeft = isTrial
            ? Math.Max(0, (int)Math.Ceiling((trialEnd - now).TotalDays))
            : activeStillValid && currentPeriodEnd.HasValue
                ? Math.Max(0, (int)Math.Ceiling((periodEnd - now).TotalDays))
                : 0;

        var message = normalized switch
        {
            "active" when hasAccess => "Assinatura ativa.",
            "active" => "Sua assinatura venceu. Escolha um plano para continuar usando o sistema.",
            "pending" => "Pagamento pendente no Mercado Pago.",
            "trialing" when hasAccess => daysLeft <= 1
                ? "Seu teste gratuito termina hoje."
                : $"Você ainda tem {daysLeft} dias de teste gratuito.",
            "expired" => "Seu teste gratuito terminou. Escolha um plano para continuar usando o sistema.",
            "paused" => "Sua assinatura está pausada no Mercado Pago.",
            "canceled" => "Sua assinatura foi cancelada.",
            _ => "Assinatura não ativa."
        };

        return new BillingStatusDto(
            normalized,
            hasAccess,
            isTrial,
            trialStartedAt,
            trialEndsAt,
            currentPeriodEnd,
            daysLeft,
            planCode,
            providerSubscriptionId,
            initPoint,
            message);
    }

    private static string NormalizeProviderStatus(string? status)
    {
        var normalized = (status ?? string.Empty).Trim().ToLowerInvariant();
        return normalized switch
        {
            "authorized" or "active" or "approved" => "active",
            "trialing" => "trialing",
            "pending" or "pending_payment" or "in_process" => "pending",
            "paused" => "paused",
            "cancelled" or "canceled" or "cancel" => "canceled",
            "expired" => "expired",
            _ => string.IsNullOrWhiteSpace(normalized) ? "pending" : normalized
        };
    }
}
