using System.Text.Json;
using JsnFinances.Api.Billing;
using JsnFinances.Api.Domain;
using Npgsql;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    public async Task<IReadOnlyList<BillingPlanDto>> ListBillingPlansAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await EnsurePixBillingSchemaAsync(connection);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            select code, nome, descricao, valor, moeda, frequency, frequency_type, destaque
            from public.billing_plans
            where ativo = true and lower(code) in ('mensal', 'anual')
            order by case lower(code) when 'mensal' then 1 when 'anual' then 2 else 99 end
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
        await EnsurePixBillingSchemaAsync(connection);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            select code, nome, descricao, valor, moeda, frequency, frequency_type, destaque
            from public.billing_plans
            where ativo = true and lower(code) in ('mensal', 'anual') and lower(code) = lower(@code)
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
        await EnsurePixBillingSchemaAsync(connection);

        await using (var insert = connection.CreateCommand())
        {
            insert.CommandText = """
                insert into public.user_subscriptions
                  (id_usuario, status, provider, trial_started_at, trial_ends_at)
                values
                  (@userId, 'trialing', 'manual', timezone('utc', now()), timezone('utc', now()) + interval '3 days')
                on conflict (id_usuario) do nothing
                """;
            Add(insert, "userId", userId);
            await insert.ExecuteNonQueryAsync();
        }

        await using (var expire = connection.CreateCommand())
        {
            expire.CommandText = """
                update public.user_subscriptions
                set status = 'expired', atualizado_em = timezone('utc', now())
                where id_usuario = @userId
                  and status = 'active'
                  and current_period_end is not null
                  and current_period_end < timezone('utc', now())
                """;
            Add(expire, "userId", userId);
            await expire.ExecuteNonQueryAsync();
        }

        return await ReadBillingStatusAsync(connection, userId);
    }

    public async Task<PixChargeDto> SavePixChargeAsync(Guid userId, BillingPlanDto plan, Guid chargeId, MercadoPagoCreatePixPaymentResult pix)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await EnsurePixBillingSchemaAsync(connection);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.billing_pix_charges
              (id, user_id, plan_code, amount, currency, mercado_pago_payment_id, mercado_pago_external_reference,
               payment_status, qr_code, qr_code_base64, pix_copy_paste, ticket_url, expires_at, paid_at, raw_payload)
            values
              (@id, @userId, @planCode, @amount, @currency, @paymentId, @externalReference,
               @status, @qrCode, @qrCodeBase64, @pixCopyPaste, @ticketUrl, @expiresAt, @paidAt, cast(@rawPayload as jsonb))
            """;
        Add(command, "id", chargeId);
        Add(command, "userId", userId);
        Add(command, "planCode", plan.Code);
        Add(command, "amount", plan.Valor);
        Add(command, "currency", plan.Moeda);
        Add(command, "paymentId", pix.Id);
        Add(command, "externalReference", pix.ExternalReference);
        Add(command, "status", NormalizePaymentStatus(pix.Status));
        Add(command, "qrCode", pix.QrCode);
        Add(command, "qrCodeBase64", pix.QrCodeBase64);
        Add(command, "pixCopyPaste", pix.QrCode);
        Add(command, "ticketUrl", pix.TicketUrl);
        Add(command, "expiresAt", pix.ExpiresAt);
        Add(command, "paidAt", pix.PaidAt);
        Add(command, "rawPayload", string.IsNullOrWhiteSpace(pix.RawPayload) ? "{}" : pix.RawPayload);
        await command.ExecuteNonQueryAsync();

        var charge = await GetPixChargeAsync(userId, chargeId) ?? throw new InvalidOperationException("Não foi possível salvar a cobrança PIX.");
        if (charge.PaymentStatus == "approved")
        {
            await ApplyApprovedPixChargeAsync(connection, charge, pix.RawPayload);
            charge = await GetPixChargeAsync(userId, chargeId) ?? charge;
        }

        return charge;
    }

    public async Task<PixChargeDto?> GetPixChargeAsync(Guid userId, Guid chargeId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await EnsurePixBillingSchemaAsync(connection);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, user_id, plan_code, amount, currency, mercado_pago_payment_id, mercado_pago_external_reference,
                   payment_status, qr_code, qr_code_base64, pix_copy_paste, ticket_url, expires_at, paid_at,
                   access_until, created_at, updated_at
            from public.billing_pix_charges
            where id = @id and user_id = @userId
            limit 1
            """;
        Add(command, "id", chargeId);
        Add(command, "userId", userId);

        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadPixCharge(reader) : null;
    }

    public async Task<PixChargeDto> UpdatePixChargeFromPaymentAsync(Guid userId, Guid chargeId, MercadoPagoPaymentResult payment)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await EnsurePixBillingSchemaAsync(connection);

        var charge = await UpdatePixChargeFromPaymentAsync(connection, userId, chargeId, payment)
            ?? throw new KeyNotFoundException("Cobrança PIX não encontrada para este usuário.");

        if (charge.PaymentStatus == "approved")
        {
            await ApplyApprovedPixChargeAsync(connection, charge, payment.RawPayload);
            charge = await ReadPixChargeByIdAsync(connection, userId, chargeId) ?? charge;
        }

        return charge;
    }

    public async Task UpdatePixChargeFromWebhookPaymentAsync(MercadoPagoPaymentResult payment)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await EnsurePixBillingSchemaAsync(connection);

        var charge = await UpdatePixChargeFromPaymentAsync(connection, null, null, payment);
        if (charge?.PaymentStatus == "approved")
        {
            await ApplyApprovedPixChargeAsync(connection, charge, payment.RawPayload);
        }
    }

    public async Task CancelUserAccessAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await EnsurePixBillingSchemaAsync(connection);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            update public.user_subscriptions
            set status = 'canceled', atualizado_em = timezone('utc', now())
            where id_usuario = @userId
            """;
        Add(command, "userId", userId);
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
        await EnsurePixBillingSchemaAsync(connection);

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

    private static async Task<PixChargeDto?> UpdatePixChargeFromPaymentAsync(NpgsqlConnection connection, Guid? userId, Guid? chargeId, MercadoPagoPaymentResult payment)
    {
        var status = NormalizePaymentStatus(payment.Status);
        await using var command = connection.CreateCommand();
        var extraWhere = userId.HasValue && chargeId.HasValue
            ? " and user_id = @userId and id = @chargeId"
            : string.Empty;
        command.CommandText = $"""
            update public.billing_pix_charges
            set payment_status = @status,
                paid_at = case when @status = 'approved' then coalesce(@paidAt, paid_at, timezone('utc', now())) else paid_at end,
                raw_payload = cast(@rawPayload as jsonb),
                updated_at = timezone('utc', now())
            where mercado_pago_payment_id = @paymentId{extraWhere}
            returning id, user_id, plan_code, amount, currency, mercado_pago_payment_id, mercado_pago_external_reference,
                      payment_status, qr_code, qr_code_base64, pix_copy_paste, ticket_url, expires_at, paid_at,
                      access_until, created_at, updated_at
            """;
        Add(command, "status", status);
        Add(command, "paidAt", payment.PaidAt);
        Add(command, "rawPayload", string.IsNullOrWhiteSpace(payment.RawPayload) ? "{}" : payment.RawPayload);
        Add(command, "paymentId", payment.Id);
        if (userId.HasValue) Add(command, "userId", userId.Value);
        if (chargeId.HasValue) Add(command, "chargeId", chargeId.Value);

        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadPixCharge(reader) : null;
    }

    private static async Task<PixChargeDto?> ReadPixChargeByIdAsync(NpgsqlConnection connection, Guid userId, Guid chargeId)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, user_id, plan_code, amount, currency, mercado_pago_payment_id, mercado_pago_external_reference,
                   payment_status, qr_code, qr_code_base64, pix_copy_paste, ticket_url, expires_at, paid_at,
                   access_until, created_at, updated_at
            from public.billing_pix_charges
            where id = @id and user_id = @userId
            limit 1
            """;
        Add(command, "id", chargeId);
        Add(command, "userId", userId);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadPixCharge(reader) : null;
    }

    private static async Task ApplyApprovedPixChargeAsync(NpgsqlConnection connection, PixChargeDto charge, string rawPayload)
    {
        var paidAt = charge.PaidAt ?? DateTimeOffset.UtcNow;
        var durationDays = charge.PlanCode.Equals("anual", StringComparison.OrdinalIgnoreCase) ? 365 : 30;

        await using var subscription = connection.CreateCommand();
        subscription.CommandText = """
            insert into public.user_subscriptions
              (id_usuario, status, plan_code, provider, current_period_end, last_synced_at, provider_raw_payload)
            values
              (@userId, 'active', @planCode, 'mercadopago', @paidAt + (@durationDays * interval '1 day'), timezone('utc', now()), cast(@rawPayload as jsonb))
            on conflict (id_usuario)
            do update set
              status = 'active',
              plan_code = excluded.plan_code,
              provider = 'mercadopago',
              current_period_end = (
                case
                  when public.user_subscriptions.current_period_end is not null
                   and public.user_subscriptions.current_period_end > @paidAt
                  then public.user_subscriptions.current_period_end
                  else @paidAt
                end + (@durationDays * interval '1 day')
              ),
              provider_raw_payload = cast(@rawPayload as jsonb),
              last_synced_at = timezone('utc', now()),
              atualizado_em = timezone('utc', now())
            returning current_period_end
            """;
        Add(subscription, "userId", charge.UserId);
        Add(subscription, "planCode", charge.PlanCode);
        Add(subscription, "paidAt", paidAt);
        Add(subscription, "durationDays", durationDays);
        Add(subscription, "rawPayload", string.IsNullOrWhiteSpace(rawPayload) ? "{}" : rawPayload);
        var accessUntilObj = await subscription.ExecuteScalarAsync();
        var accessUntil = accessUntilObj switch
        {
            DateTimeOffset dto => dto,
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
            _ => paidAt.AddDays(durationDays)
        };

        await using var updateCharge = connection.CreateCommand();
        updateCharge.CommandText = """
            update public.billing_pix_charges
            set access_until = @accessUntil,
                updated_at = timezone('utc', now())
            where id = @id
            """;
        Add(updateCharge, "accessUntil", accessUntil);
        Add(updateCharge, "id", charge.Id);
        await updateCharge.ExecuteNonQueryAsync();

        await using var payment = connection.CreateCommand();
        payment.CommandText = """
            insert into public.payments
              (id_usuario, provider, provider_payment_id, provider_subscription_id, status, amount, currency, paid_at, raw_payload)
            values
              (@userId, 'mercadopago', @paymentId, null, 'approved', @amount, @currency, @paidAt, cast(@rawPayload as jsonb))
            on conflict (provider_payment_id)
            do update set
              status = excluded.status,
              amount = excluded.amount,
              currency = excluded.currency,
              paid_at = excluded.paid_at,
              raw_payload = excluded.raw_payload
            """;
        Add(payment, "userId", charge.UserId);
        Add(payment, "paymentId", charge.MercadoPagoPaymentId);
        Add(payment, "amount", charge.Amount);
        Add(payment, "currency", charge.Currency);
        Add(payment, "paidAt", paidAt);
        Add(payment, "rawPayload", string.IsNullOrWhiteSpace(rawPayload) ? "{}" : rawPayload);
        await payment.ExecuteNonQueryAsync();
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
            return new BillingStatusDto("expired", false, false, null, null, null, 0, null, null, null, "Escolha um plano para liberar o acesso premium.");
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

    var trialStillValid =
        normalized == "trialing"
        && trialEndsAt.HasValue
        && now <= trialEndsAt.Value;

    var activeStillValid =
        normalized == "active"
        && currentPeriodEnd.HasValue
        && now <= currentPeriodEnd.Value;

    var hasAccess = trialStillValid || activeStillValid;

    var endDate = activeStillValid
        ? currentPeriodEnd
        : trialStillValid
            ? trialEndsAt
            : currentPeriodEnd ?? trialEndsAt;

    var daysLeft = hasAccess && endDate.HasValue
        ? Math.Max(0, (int)Math.Ceiling((endDate.Value - now).TotalDays))
        : 0;

    var finalStatus =
        activeStillValid ? "active" :
        trialStillValid ? "trialing" :
        normalized is "active" or "trialing" ? "expired" :
        normalized;

    var message = finalStatus switch
    {
        "active" => "Acesso premium ativo.",
        "trialing" => "Teste grátis ativo.",
        "pending" => "Pagamento PIX pendente.",
        "canceled" => "Acesso premium cancelado.",
        _ => "Escolha um plano e pague via PIX para liberar o acesso premium."
    };

    return new BillingStatusDto(
        finalStatus,
        hasAccess,
        trialStillValid,
        trialStartedAt,
        trialEndsAt,
        currentPeriodEnd,
        daysLeft,
        planCode,
        providerSubscriptionId,
        initPoint,
        message);
}

    private static PixChargeDto ReadPixCharge(NpgsqlDataReader reader)
        => new(
            reader.GetGuid(0),
            reader.GetGuid(1),
            reader.GetString(2),
            reader.GetDecimal(3),
            reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.GetString(6),
            reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : reader.GetString(11),
            reader.IsDBNull(12) ? null : reader.GetFieldValue<DateTimeOffset>(12),
            reader.IsDBNull(13) ? null : reader.GetFieldValue<DateTimeOffset>(13),
            reader.IsDBNull(14) ? null : reader.GetFieldValue<DateTimeOffset>(14),
            reader.GetFieldValue<DateTimeOffset>(15),
            reader.GetFieldValue<DateTimeOffset>(16));

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
            _ => string.IsNullOrWhiteSpace(normalized) ? "expired" : normalized
        };
    }

    private static string NormalizePaymentStatus(string? status)
    {
        var normalized = (status ?? string.Empty).Trim().ToLowerInvariant();
        return normalized switch
        {
            "approved" or "authorized" or "active" => "approved",
            "pending" or "pending_payment" or "in_process" => "pending",
            "cancelled" or "canceled" or "cancel" => "canceled",
            "expired" => "expired",
            "rejected" => "rejected",
            _ => string.IsNullOrWhiteSpace(normalized) ? "pending" : normalized
        };
    }

    private static async Task EnsurePixBillingSchemaAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            create extension if not exists pgcrypto;

            create table if not exists public.billing_plans (
                id uuid primary key default gen_random_uuid(),
                code text not null unique,
                nome text not null,
                descricao text not null default '',
                valor numeric(12,2) not null check (valor >= 0),
                moeda text not null default 'BRL',
                frequency integer not null default 1 check (frequency > 0),
                frequency_type text not null check (frequency_type in ('days', 'months')),
                ativo boolean not null default true,
                destaque boolean not null default false,
                ordem integer not null default 0,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now())
            );

            create table if not exists public.user_subscriptions (
                id uuid primary key default gen_random_uuid(),
                id_usuario uuid not null unique,
                status text not null default 'expired',
                trial_started_at timestamptz,
                trial_ends_at timestamptz,
                current_period_end timestamptz,
                plan_code text references public.billing_plans(code),
                provider text not null default 'mercadopago',
                provider_customer_id text,
                provider_subscription_id text unique,
                provider_payer_email text,
                init_point text,
                provider_raw_payload jsonb not null default '{}'::jsonb,
                last_synced_at timestamptz,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now())
            );

            create table if not exists public.payments (
                id uuid primary key default gen_random_uuid(),
                id_usuario uuid not null,
                subscription_id uuid references public.user_subscriptions(id) on delete set null,
                provider text not null default 'mercadopago',
                provider_payment_id text unique,
                provider_subscription_id text,
                status text,
                amount numeric(12,2),
                currency text default 'BRL',
                paid_at timestamptz,
                raw_payload jsonb not null default '{}'::jsonb,
                criado_em timestamptz not null default timezone('utc', now())
            );

            create table if not exists public.billing_events (
                id uuid primary key default gen_random_uuid(),
                provider text not null default 'mercadopago',
                event_type text not null,
                event_action text,
                resource_id text,
                request_id text,
                signature_valid boolean not null default false,
                payload jsonb not null default '{}'::jsonb,
                processado_em timestamptz not null default timezone('utc', now())
            );

            create table if not exists public.billing_pix_charges (
                id uuid primary key,
                user_id uuid not null,
                plan_code text not null references public.billing_plans(code),
                amount numeric(12,2) not null,
                currency text not null default 'BRL',
                mercado_pago_payment_id text unique,
                mercado_pago_external_reference text not null unique,
                payment_status text not null default 'pending',
                qr_code text,
                qr_code_base64 text,
                pix_copy_paste text,
                ticket_url text,
                expires_at timestamptz,
                paid_at timestamptz,
                access_until timestamptz,
                raw_payload jsonb not null default '{}'::jsonb,
                created_at timestamptz not null default timezone('utc', now()),
                updated_at timestamptz not null default timezone('utc', now())
            );

            create index if not exists ix_billing_pix_charges_user on public.billing_pix_charges(user_id);
            create index if not exists ix_billing_pix_charges_payment on public.billing_pix_charges(mercado_pago_payment_id);
            create index if not exists ix_billing_pix_charges_status on public.billing_pix_charges(payment_status);
            create index if not exists ix_user_subscriptions_user on public.user_subscriptions(id_usuario);
            create index if not exists ix_user_subscriptions_status on public.user_subscriptions(status);
            create index if not exists ix_billing_events_resource on public.billing_events(resource_id);
            create index if not exists ix_billing_events_request on public.billing_events(request_id);

            insert into public.billing_plans (code, nome, descricao, valor, moeda, frequency, frequency_type, ativo, destaque, ordem)
            values
                ('mensal', 'Plano Mensal', 'Pagamento avulso via PIX com liberação por 30 dias.', 19.90, 'BRL', 30, 'days', true, false, 1),
                ('anual', 'Plano Anual', 'Pagamento avulso via PIX com liberação por 365 dias.', 199.90, 'BRL', 365, 'days', true, true, 2)
            on conflict (code)
            do update set
                nome = excluded.nome,
                descricao = excluded.descricao,
                moeda = excluded.moeda,
                frequency = excluded.frequency,
                frequency_type = excluded.frequency_type,
                ativo = true,
                destaque = excluded.destaque,
                ordem = excluded.ordem,
                atualizado_em = timezone('utc', now());

            update public.billing_plans
            set ativo = false, atualizado_em = timezone('utc', now())
            where lower(code) not in ('mensal', 'anual');
            """;
        await command.ExecuteNonQueryAsync();
    }
}
