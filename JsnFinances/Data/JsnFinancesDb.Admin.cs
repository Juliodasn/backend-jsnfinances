using System.Text.Json;
using JsnFinances.Api.Domain;
using Npgsql;
using NpgsqlTypes;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    public async Task<AdminUserVerificationDto?> GetAdminUserVerificationAsync(Guid userId, string email)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, email, (email_confirmed_at is not null) as email_confirmed
            from auth.users
            where id = @userId and lower(email) = lower(@email)
            limit 1
            """;
        Add(command, "userId", userId);
        Add(command, "email", email);

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;
        return new AdminUserVerificationDto(reader.GetGuid(0), reader.GetString(1), reader.GetBoolean(2));
    }

    public async Task InsertAdminAccessLogAsync(Guid userId, string email, string path, string method, string? ipAddress, string? userAgent)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.admin_access_logs (id_usuario, email, path, method, ip_address, user_agent)
            values (@userId, @email, @path, @method, @ipAddress, @userAgent)
            """;
        Add(command, "userId", userId);
        Add(command, "email", email);
        Add(command, "path", path);
        Add(command, "method", method);
        Add(command, "ipAddress", ipAddress);
        Add(command, "userAgent", userAgent);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<AdminDashboardDto> GetAdminDashboardAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            with users_base as (
                select id, created_at from auth.users
            ),
            subscription_base as (
                select s.*, p.valor, p.frequency, p.frequency_type
                from public.user_subscriptions s
                left join public.billing_plans p on p.code = s.plan_code
            ),
            approved_payments as (
                select * from public.payments
                where lower(coalesce(status, '')) in ('approved', 'authorized', 'paid', 'accredited')
            ),
            mrr_base as (
                select coalesce(sum(
                    case
                        when s.status = 'active' and s.frequency_type = 'months' and coalesce(s.frequency, 0) > 0 then s.valor / s.frequency
                        when s.status = 'active' and s.frequency_type = 'days' and coalesce(s.frequency, 0) > 0 then s.valor * (30.0 / s.frequency)
                        else 0
                    end
                ), 0)::numeric(14,2) as mrr
                from subscription_base s
            )
            select
                count(u.id)::int,
                count(*) filter (where coalesce(s.status, 'trialing') = 'trialing')::int,
                count(*) filter (where s.status = 'active')::int,
                count(*) filter (where s.status = 'pending')::int,
                count(*) filter (where s.status = 'expired')::int,
                count(*) filter (where s.status = 'canceled')::int,
                (select count(*)::int from public.payments),
                (select count(*)::int from approved_payments),
                coalesce((select sum(amount) from approved_payments), 0)::numeric(14,2),
                (select mrr from mrr_base),
                ((select mrr from mrr_base) * 12)::numeric(14,2),
                case when count(u.id) = 0 then 0 else round((count(*) filter (where s.status = 'active')::numeric / count(u.id)::numeric) * 100, 2) end,
                coalesce(round(avg(extract(epoch from (now() - u.created_at)) / 86400.0), 1), 0)::numeric(14,1),
                (select max(processado_em) from public.billing_events),
                now()
            from users_base u
            left join subscription_base s on s.id_usuario = u.id
            """;

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return new AdminDashboardDto(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, DateTimeOffset.UtcNow);
        }

        return new AdminDashboardDto(
            reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4), reader.GetInt32(5),
            reader.GetInt32(6), reader.GetInt32(7), reader.GetDecimal(8), reader.GetDecimal(9), reader.GetDecimal(10), reader.GetDecimal(11), reader.GetDecimal(12),
            ReadNullableDateTimeOffset(reader, 13), reader.GetFieldValue<DateTimeOffset>(14));
    }

    public async Task<PagedResultDto<AdminUserDto>> ListAdminUsersAsync(string? search, string? status, int page, int pageSize)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);
        var offset = (page - 1) * pageSize;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            with usage_base as (
                select id_usuario,
                       sum(total_entries)::int as total_entries,
                       sum(total_exits)::int as total_exits,
                       sum(total_categories)::int as total_categories,
                       sum(total_goals)::int as total_goals,
                       max(last_activity_at) as last_activity_at
                from (
                    select id_usuario, count(*) total_entries, 0 total_exits, 0 total_categories, 0 total_goals, max(criado_em) last_activity_at from public.entradas group by id_usuario
                    union all select id_usuario, 0, count(*), 0, 0, max(criado_em) from public.saidas group by id_usuario
                    union all select id_usuario, 0, 0, count(*), 0, max(criado_em) from public.categorias group by id_usuario
                    union all select id_usuario, 0, 0, 0, count(*), max(criado_em) from public.metas group by id_usuario
                ) x
                group by id_usuario
            ),
            users_enriched as (
                select u.id,
                       coalesce(p.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name') as nome,
                       u.email, u.created_at, u.last_sign_in_at,
                       (u.email_confirmed_at is not null) as email_confirmed,
                       coalesce(s.status, 'sem_assinatura') as subscription_status,
                       s.plan_code, s.trial_started_at, s.trial_ends_at, s.current_period_end,
                       usage.last_activity_at,
                       greatest(0, floor(extract(epoch from (now() - u.created_at)) / 86400.0))::int as days_using,
                       coalesce(usage.total_entries, 0) as total_entries,
                       coalesce(usage.total_exits, 0) as total_exits,
                       coalesce(usage.total_categories, 0) as total_categories,
                       coalesce(usage.total_goals, 0) as total_goals
                from auth.users u
                left join public.perfis p on p.id_usuario = u.id
                left join public.user_subscriptions s on s.id_usuario = u.id
                left join usage_base usage on usage.id_usuario = u.id
                where (@search is null or u.email ilike '%' || @search || '%' or coalesce(p.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name', '') ilike '%' || @search || '%')
                  and (@status is null or coalesce(s.status, 'sem_assinatura') = @status)
            )
            select *, count(*) over()::int as total_count
            from users_enriched
            order by created_at desc
            limit @limit offset @offset
            """;
        Add(command, "search", NullIfWhiteSpace(search), NpgsqlDbType.Text);
        Add(command, "status", NullIfWhiteSpace(status), NpgsqlDbType.Text);
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<AdminUserDto>();
        var total = 0;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            total = reader.GetInt32(17);
            rows.Add(new AdminUserDto(reader.GetGuid(0), ReadNullableString(reader, 1), reader.GetString(2), reader.GetFieldValue<DateTimeOffset>(3),
                ReadNullableDateTimeOffset(reader, 4), reader.GetBoolean(5), reader.GetString(6), ReadNullableString(reader, 7),
                ReadNullableDateTimeOffset(reader, 8), ReadNullableDateTimeOffset(reader, 9), ReadNullableDateTimeOffset(reader, 10), ReadNullableDateTimeOffset(reader, 11),
                reader.GetInt32(12), reader.GetInt32(13), reader.GetInt32(14), reader.GetInt32(15), reader.GetInt32(16)));
        }
        return BuildPagedResult(rows, total, page, pageSize);
    }

    public async Task<PagedResultDto<AdminSubscriptionDto>> ListAdminSubscriptionsAsync(string? search, string? status, int page, int pageSize)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);
        var offset = (page - 1) * pageSize;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select s.id, s.id_usuario,
                   coalesce(pf.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name') as nome,
                   u.email, s.status, s.plan_code, bp.nome, bp.valor, s.provider, s.provider_subscription_id, s.provider_payer_email,
                   s.trial_started_at, s.trial_ends_at, s.current_period_end, s.last_synced_at, s.criado_em, s.atualizado_em,
                   count(*) over()::int as total_count
            from public.user_subscriptions s
            left join auth.users u on u.id = s.id_usuario
            left join public.perfis pf on pf.id_usuario = s.id_usuario
            left join public.billing_plans bp on bp.code = s.plan_code
            where (@search is null or u.email ilike '%' || @search || '%' or s.provider_payer_email ilike '%' || @search || '%' or s.provider_subscription_id ilike '%' || @search || '%' or coalesce(pf.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name', '') ilike '%' || @search || '%')
              and (@status is null or s.status = @status)
            order by s.atualizado_em desc
            limit @limit offset @offset
            """;
        Add(command, "search", NullIfWhiteSpace(search), NpgsqlDbType.Text);
        Add(command, "status", NullIfWhiteSpace(status), NpgsqlDbType.Text);
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<AdminSubscriptionDto>();
        var total = 0;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            total = reader.GetInt32(17);
            rows.Add(new AdminSubscriptionDto(reader.GetGuid(0), reader.GetGuid(1), ReadNullableString(reader, 2), ReadNullableString(reader, 3), reader.GetString(4), ReadNullableString(reader, 5), ReadNullableString(reader, 6), ReadNullableDecimal(reader, 7), reader.GetString(8), ReadNullableString(reader, 9), ReadNullableString(reader, 10), ReadNullableDateTimeOffset(reader, 11), ReadNullableDateTimeOffset(reader, 12), ReadNullableDateTimeOffset(reader, 13), ReadNullableDateTimeOffset(reader, 14), reader.GetFieldValue<DateTimeOffset>(15), reader.GetFieldValue<DateTimeOffset>(16)));
        }
        return BuildPagedResult(rows, total, page, pageSize);
    }

    public async Task<PagedResultDto<AdminPaymentDto>> ListAdminPaymentsAsync(string? search, string? status, int page, int pageSize)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);
        var offset = (page - 1) * pageSize;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select p.id, p.id_usuario,
                   coalesce(pf.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name') as nome,
                   u.email, p.provider, p.provider_payment_id, p.provider_subscription_id, p.status, p.amount, p.currency, p.paid_at, p.criado_em,
                   count(*) over()::int as total_count
            from public.payments p
            left join auth.users u on u.id = p.id_usuario
            left join public.perfis pf on pf.id_usuario = p.id_usuario
            where (@search is null or u.email ilike '%' || @search || '%' or p.provider_payment_id ilike '%' || @search || '%' or p.provider_subscription_id ilike '%' || @search || '%' or coalesce(pf.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name', '') ilike '%' || @search || '%')
              and (@status is null or p.status = @status)
            order by coalesce(p.paid_at, p.criado_em) desc
            limit @limit offset @offset
            """;
        Add(command, "search", NullIfWhiteSpace(search), NpgsqlDbType.Text);
        Add(command, "status", NullIfWhiteSpace(status), NpgsqlDbType.Text);
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<AdminPaymentDto>();
        var total = 0;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            total = reader.GetInt32(12);
            rows.Add(new AdminPaymentDto(reader.GetGuid(0), reader.GetGuid(1), ReadNullableString(reader, 2), ReadNullableString(reader, 3), reader.GetString(4), ReadNullableString(reader, 5), ReadNullableString(reader, 6), ReadNullableString(reader, 7), ReadNullableDecimal(reader, 8), ReadNullableString(reader, 9), ReadNullableDateTimeOffset(reader, 10), reader.GetFieldValue<DateTimeOffset>(11)));
        }
        return BuildPagedResult(rows, total, page, pageSize);
    }

    public async Task<PagedResultDto<AdminBillingEventDto>> ListAdminBillingEventsAsync(string? search, int page, int pageSize)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);
        var offset = (page - 1) * pageSize;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, provider, event_type, event_action, resource_id, request_id, signature_valid, processado_em,
                   count(*) over()::int as total_count
            from public.billing_events
            where @search is null or provider ilike '%' || @search || '%' or event_type ilike '%' || @search || '%' or coalesce(event_action, '') ilike '%' || @search || '%' or coalesce(resource_id, '') ilike '%' || @search || '%' or coalesce(request_id, '') ilike '%' || @search || '%'
            order by processado_em desc
            limit @limit offset @offset
            """;
        Add(command, "search", NullIfWhiteSpace(search), NpgsqlDbType.Text);
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<AdminBillingEventDto>();
        var total = 0;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            total = reader.GetInt32(8);
            rows.Add(new AdminBillingEventDto(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), ReadNullableString(reader, 3), ReadNullableString(reader, 4), ReadNullableString(reader, 5), reader.GetBoolean(6), reader.GetFieldValue<DateTimeOffset>(7)));
        }
        return BuildPagedResult(rows, total, page, pageSize);
    }

    public async Task<PagedResultDto<AdminAccessLogDto>> ListAdminAccessLogsAsync(int page, int pageSize)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);
        var offset = (page - 1) * pageSize;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, id_usuario, email, path, method, ip_address, user_agent, accessed_at,
                   count(*) over()::int as total_count
            from public.admin_access_logs
            order by accessed_at desc
            limit @limit offset @offset
            """;
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<AdminAccessLogDto>();
        var total = 0;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            total = reader.GetInt32(8);
            rows.Add(new AdminAccessLogDto(reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2), reader.GetString(3), reader.GetString(4), ReadNullableString(reader, 5), ReadNullableString(reader, 6), reader.GetFieldValue<DateTimeOffset>(7)));
        }
        return BuildPagedResult(rows, total, page, pageSize);
    }

    public async Task UpsertPaymentFromProviderAsync(Guid userId, string providerPaymentId, string? providerSubscriptionId, string? status, decimal? amount, string? currency, DateTimeOffset? paidAt, string rawPayload)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.payments (id_usuario, subscription_id, provider, provider_payment_id, provider_subscription_id, status, amount, currency, paid_at, raw_payload)
            select @userId, s.id, 'mercadopago', @providerPaymentId, @providerSubscriptionId, @status, @amount, coalesce(@currency, 'BRL'), @paidAt, cast(@rawPayload as jsonb)
            from (select @userId as id_usuario) x
            left join public.user_subscriptions s on s.id_usuario = x.id_usuario and (@providerSubscriptionId is null or s.provider_subscription_id = @providerSubscriptionId)
            limit 1
            on conflict (provider_payment_id)
            do update set provider_subscription_id = excluded.provider_subscription_id, status = excluded.status, amount = excluded.amount, currency = excluded.currency, paid_at = excluded.paid_at, raw_payload = excluded.raw_payload
            """;
        Add(command, "userId", userId);
        Add(command, "providerPaymentId", providerPaymentId);
        Add(command, "providerSubscriptionId", providerSubscriptionId);
        Add(command, "status", status);
        Add(command, "amount", amount);
        Add(command, "currency", currency);
        Add(command, "paidAt", paidAt);
        Add(command, "rawPayload", string.IsNullOrWhiteSpace(rawPayload) ? "{}" : rawPayload);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<PagedResultDto<AdminOnboardingProfileDto>> ListAdminOnboardingProfilesAsync(string? search, int page, int pageSize)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);
        var offset = (page - 1) * pageSize;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select
                u.id,
                coalesce(p.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name') as nome,
                u.email,
                op.profile_type,
                op.main_goal,
                op.financial_moment,
                op.biggest_challenge,
                op.usage_frequency,
                coalesce(o.completed, false) as onboarding_completed,
                coalesce(o.skipped, false) as onboarding_skipped,
                op.created_at,
                op.updated_at,
                count(*) over()::int as total_count
            from auth.users u
            left join public.perfis p on p.id_usuario = u.id
            left join public.user_onboarding o on o.user_id = u.id
            left join public.user_onboarding_profile op on op.id_usuario = u.id
            where (@search is null
                   or u.email ilike '%' || @search || '%'
                   or coalesce(p.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name', '') ilike '%' || @search || '%'
                   or coalesce(op.profile_type, '') ilike '%' || @search || '%'
                   or coalesce(op.main_goal, '') ilike '%' || @search || '%'
                   or coalesce(op.financial_moment, '') ilike '%' || @search || '%'
                   or coalesce(op.biggest_challenge, '') ilike '%' || @search || '%'
                   or coalesce(op.usage_frequency, '') ilike '%' || @search || '%')
            order by coalesce(op.updated_at, u.created_at) desc
            limit @limit offset @offset
            """;
        Add(command, "search", NullIfWhiteSpace(search), NpgsqlDbType.Text);
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<AdminOnboardingProfileDto>();
        var total = 0;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            total = reader.GetInt32(12);
            rows.Add(new AdminOnboardingProfileDto(
                reader.GetGuid(0),
                ReadNullableString(reader, 1),
                reader.GetString(2),
                ReadNullableString(reader, 3),
                ReadNullableString(reader, 4),
                ReadNullableString(reader, 5),
                ReadNullableString(reader, 6),
                ReadNullableString(reader, 7),
                reader.GetBoolean(8),
                reader.GetBoolean(9),
                ReadNullableDateTimeOffset(reader, 10),
                ReadNullableDateTimeOffset(reader, 11)));
        }

        return BuildPagedResult(rows, total, page, pageSize);
    }


    public async Task<AdminUserDetailDto?> GetAdminUserDetailsAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            with usage_base as (
                select
                    @userId::uuid as id_usuario,
                    (select count(*)::int from public.entradas where id_usuario = @userId) as total_entries,
                    (select count(*)::int from public.saidas where id_usuario = @userId) as total_exits,
                    (select count(*)::int from public.contas where id_usuario = @userId) as total_accounts,
                    0::int as total_cards,
                    (select count(*)::int from public.categorias where id_usuario = @userId) as total_categories,
                    (select count(*)::int from public.metas where id_usuario = @userId) as total_goals,
                    coalesce((select sum(valor) from public.entradas where id_usuario = @userId), 0)::numeric(14,2)
                      + coalesce((select sum(valor) from public.saidas where id_usuario = @userId), 0)::numeric(14,2) as total_moved,
                    greatest(
                      coalesce((select max(criado_em) from public.entradas where id_usuario = @userId), '-infinity'::timestamptz),
                      coalesce((select max(criado_em) from public.saidas where id_usuario = @userId), '-infinity'::timestamptz),
                      coalesce((select max(criado_em) from public.contas where id_usuario = @userId), '-infinity'::timestamptz),
                      coalesce((select max(criado_em) from public.categorias where id_usuario = @userId), '-infinity'::timestamptz),
                      coalesce((select max(criado_em) from public.metas where id_usuario = @userId), '-infinity'::timestamptz)
                    ) as last_activity_at
            )
            select
                u.id,
                coalesce(p.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name') as nome,
                u.email,
                u.created_at,
                u.last_sign_in_at,
                (u.email_confirmed_at is not null) as email_confirmed,
                u.banned_until,
                usage.total_entries,
                usage.total_exits,
                usage.total_accounts,
                usage.total_cards,
                usage.total_categories,
                usage.total_goals,
                usage.total_moved,
                nullif(usage.last_activity_at, '-infinity'::timestamptz) as last_activity_at,
                s.id as subscription_id,
                s.status,
                s.plan_code,
                bp.nome as plan_name,
                bp.valor as plan_value,
                coalesce(s.provider, 'manual') as provider,
                s.provider_subscription_id,
                s.provider_payer_email,
                s.trial_started_at,
                s.trial_ends_at,
                s.current_period_end,
                s.last_synced_at,
                s.criado_em as subscription_created_at,
                s.atualizado_em as subscription_updated_at,
                coalesce(o.completed, false) as onboarding_completed,
                coalesce(o.skipped, false) as onboarding_skipped,
                op.profile_type,
                op.main_goal,
                op.financial_moment,
                op.biggest_challenge,
                op.usage_frequency,
                op.updated_at as onboarding_updated_at
            from auth.users u
            left join public.perfis p on p.id_usuario = u.id
            left join usage_base usage on usage.id_usuario = u.id
            left join public.user_subscriptions s on s.id_usuario = u.id
            left join public.billing_plans bp on bp.code = s.plan_code
            left join public.user_onboarding o on o.user_id = u.id
            left join public.user_onboarding_profile op on op.id_usuario = u.id
            where u.id = @userId
            limit 1
            """;
        Add(command, "userId", userId, NpgsqlDbType.Uuid);

        AdminUserBasicDetailDto user;
        AdminUserUsageSummaryDto usage;
        AdminSubscriptionDto? subscription;
        AdminOnboardingDetailDto onboarding;

        await using (var reader = await command.ExecuteReaderAsync())
        {
            if (!await reader.ReadAsync()) return null;

            var blockedUntil = ReadNullableDateTimeOffset(reader, 6);
            user = new AdminUserBasicDetailDto(
                reader.GetGuid(0),
                ReadNullableString(reader, 1),
                reader.GetString(2),
                reader.GetFieldValue<DateTimeOffset>(3),
                ReadNullableDateTimeOffset(reader, 4),
                reader.GetBoolean(5),
                blockedUntil.HasValue && blockedUntil.Value > DateTimeOffset.UtcNow,
                blockedUntil);

            usage = new AdminUserUsageSummaryDto(
                reader.GetInt32(7),
                reader.GetInt32(8),
                reader.GetInt32(9),
                reader.GetInt32(10),
                reader.GetInt32(11),
                reader.GetInt32(12),
                reader.GetDecimal(13),
                ReadNullableDateTimeOffset(reader, 14));

            subscription = reader.IsDBNull(15)
                ? null
                : new AdminSubscriptionDto(
                    reader.GetGuid(15),
                    reader.GetGuid(0),
                    ReadNullableString(reader, 1),
                    reader.GetString(2),
                    ReadNullableString(reader, 16) ?? "sem_assinatura",
                    ReadNullableString(reader, 17),
                    ReadNullableString(reader, 18),
                    ReadNullableDecimal(reader, 19),
                    ReadNullableString(reader, 20) ?? "manual",
                    ReadNullableString(reader, 21),
                    ReadNullableString(reader, 22),
                    ReadNullableDateTimeOffset(reader, 23),
                    ReadNullableDateTimeOffset(reader, 24),
                    ReadNullableDateTimeOffset(reader, 25),
                    ReadNullableDateTimeOffset(reader, 26),
                    ReadNullableDateTimeOffset(reader, 27) ?? reader.GetFieldValue<DateTimeOffset>(3),
                    ReadNullableDateTimeOffset(reader, 28) ?? reader.GetFieldValue<DateTimeOffset>(3));

            onboarding = new AdminOnboardingDetailDto(
                reader.GetBoolean(29),
                reader.GetBoolean(30),
                ReadNullableString(reader, 31),
                ReadNullableString(reader, 32),
                ReadNullableString(reader, 33),
                ReadNullableString(reader, 34),
                ReadNullableString(reader, 35),
                ReadNullableDateTimeOffset(reader, 36));
        }

        var payments = await ListAdminPaymentsByUserAsync(connection, userId);
        var logs = await ListAdminUserActionLogsAsync(connection, userId);

        return new AdminUserDetailDto(user, usage, subscription, onboarding, payments, logs);
    }

    public async Task<bool> SetAdminUserBlockAsync(Guid adminUserId, string adminEmail, Guid targetUserId, bool blocked, string? reason)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            update auth.users
            set banned_until = case when @blocked then timezone('utc', now()) + interval '100 years' else null end,
                updated_at = timezone('utc', now())
            where id = @targetUserId
            """;
        Add(command, "blocked", blocked);
        Add(command, "targetUserId", targetUserId, NpgsqlDbType.Uuid);
        var affected = await command.ExecuteNonQueryAsync();
        if (affected > 0)
        {
            await InsertAdminUserActionLogAsync(connection, adminUserId, adminEmail, targetUserId, blocked ? "BLOCK_USER" : "UNBLOCK_USER", reason, new { blocked });
        }
        return affected > 0;
    }

    public async Task<bool> ActivateManualAccessAsync(Guid adminUserId, string adminEmail, Guid targetUserId, string? planCode, int? days, string? reason)
    {
        var normalizedPlan = string.IsNullOrWhiteSpace(planCode) ? "mensal" : planCode.Trim().ToLowerInvariant();
        var normalizedDays = Math.Clamp(days ?? (normalizedPlan == "anual" ? 365 : 30), 1, 3650);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.user_subscriptions
              (id_usuario, status, plan_code, provider, current_period_end, last_synced_at, provider_raw_payload)
            select @targetUserId, 'active', bp.code, 'manual', timezone('utc', now()) + (@days * interval '1 day'), timezone('utc', now()), cast(@payload as jsonb)
            from public.billing_plans bp
            where lower(bp.code) = lower(@planCode)
            limit 1
            on conflict (id_usuario)
            do update set
              status = 'active',
              plan_code = excluded.plan_code,
              provider = 'manual',
              current_period_end = excluded.current_period_end,
              provider_raw_payload = excluded.provider_raw_payload,
              last_synced_at = timezone('utc', now()),
              atualizado_em = timezone('utc', now())
            """;
        Add(command, "targetUserId", targetUserId, NpgsqlDbType.Uuid);
        Add(command, "planCode", normalizedPlan, NpgsqlDbType.Text);
        Add(command, "days", normalizedDays);
        Add(command, "payload", JsonSerializer.Serialize(new { source = "admin", action = "activate_manual_access", days = normalizedDays }), NpgsqlDbType.Text);
        var affected = await command.ExecuteNonQueryAsync();
        if (affected > 0)
        {
            await InsertAdminUserActionLogAsync(connection, adminUserId, adminEmail, targetUserId, "ACTIVATE_MANUAL_ACCESS", reason, new { plan_code = normalizedPlan, days = normalizedDays });
        }
        return affected > 0;
    }

    public async Task<bool> ExtendTrialAsync(Guid adminUserId, string adminEmail, Guid targetUserId, int? days, string? reason)
    {
        var normalizedDays = Math.Clamp(days ?? 3, 1, 365);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.user_subscriptions
              (id_usuario, status, provider, trial_started_at, trial_ends_at, last_synced_at, provider_raw_payload)
            values
              (@targetUserId, 'trialing', 'manual', timezone('utc', now()), timezone('utc', now()) + (@days * interval '1 day'), timezone('utc', now()), cast(@payload as jsonb))
            on conflict (id_usuario)
            do update set
              status = 'trialing',
              provider = 'manual',
              trial_started_at = coalesce(public.user_subscriptions.trial_started_at, timezone('utc', now())),
              trial_ends_at = greatest(coalesce(public.user_subscriptions.trial_ends_at, timezone('utc', now())), timezone('utc', now())) + (@days * interval '1 day'),
              provider_raw_payload = excluded.provider_raw_payload,
              last_synced_at = timezone('utc', now()),
              atualizado_em = timezone('utc', now())
            """;
        Add(command, "targetUserId", targetUserId, NpgsqlDbType.Uuid);
        Add(command, "days", normalizedDays);
        Add(command, "payload", JsonSerializer.Serialize(new { source = "admin", action = "extend_trial", days = normalizedDays }), NpgsqlDbType.Text);
        var affected = await command.ExecuteNonQueryAsync();
        if (affected > 0)
        {
            await InsertAdminUserActionLogAsync(connection, adminUserId, adminEmail, targetUserId, "EXTEND_TRIAL", reason, new { days = normalizedDays });
        }
        return affected > 0;
    }

    public async Task<bool> ChangeUserPlanAsync(Guid adminUserId, string adminEmail, Guid targetUserId, string? planCode, string? reason)
    {
        var normalizedPlan = string.IsNullOrWhiteSpace(planCode) ? "mensal" : planCode.Trim().ToLowerInvariant();
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            with plan as (
                select code,
                       case when frequency_type = 'months' then greatest(frequency, 1) * 30 else greatest(frequency, 1) end as duration_days
                from public.billing_plans
                where ativo = true and lower(code) = lower(@planCode)
                limit 1
            )
            insert into public.user_subscriptions
              (id_usuario, status, plan_code, provider, current_period_end, last_synced_at, provider_raw_payload)
            select @targetUserId, 'active', code, 'manual', timezone('utc', now()) + (duration_days * interval '1 day'), timezone('utc', now()), cast(@payload as jsonb)
            from plan
            on conflict (id_usuario)
            do update set
              plan_code = excluded.plan_code,
              provider = 'manual',
              status = case when public.user_subscriptions.status in ('active', 'trialing', 'pending') then public.user_subscriptions.status else 'active' end,
              current_period_end = coalesce(public.user_subscriptions.current_period_end, excluded.current_period_end),
              provider_raw_payload = excluded.provider_raw_payload,
              last_synced_at = timezone('utc', now()),
              atualizado_em = timezone('utc', now())
            """;
        Add(command, "targetUserId", targetUserId, NpgsqlDbType.Uuid);
        Add(command, "planCode", normalizedPlan, NpgsqlDbType.Text);
        Add(command, "payload", JsonSerializer.Serialize(new { source = "admin", action = "change_plan", plan_code = normalizedPlan }), NpgsqlDbType.Text);
        var affected = await command.ExecuteNonQueryAsync();
        if (affected > 0)
        {
            await InsertAdminUserActionLogAsync(connection, adminUserId, adminEmail, targetUserId, "CHANGE_PLAN", reason, new { plan_code = normalizedPlan });
        }
        return affected > 0;
    }

    public async Task<bool> CancelAdminUserSubscriptionAsync(Guid adminUserId, string adminEmail, Guid targetUserId, string? reason)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            update public.user_subscriptions
            set status = 'canceled',
                current_period_end = timezone('utc', now()),
                last_synced_at = timezone('utc', now()),
                atualizado_em = timezone('utc', now())
            where id_usuario = @targetUserId
            """;
        Add(command, "targetUserId", targetUserId, NpgsqlDbType.Uuid);
        var affected = await command.ExecuteNonQueryAsync();
        if (affected > 0)
        {
            await InsertAdminUserActionLogAsync(connection, adminUserId, adminEmail, targetUserId, "CANCEL_SUBSCRIPTION", reason, null);
        }
        return affected > 0;
    }

    private static async Task<IReadOnlyList<AdminPaymentDto>> ListAdminPaymentsByUserAsync(NpgsqlConnection connection, Guid userId)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select p.id, p.id_usuario,
                   coalesce(pf.nome_completo, u.raw_user_meta_data ->> 'nome_completo', u.raw_user_meta_data ->> 'name') as nome,
                   u.email, p.provider, p.provider_payment_id, p.provider_subscription_id, p.status, p.amount, p.currency, p.paid_at, p.criado_em
            from public.payments p
            left join auth.users u on u.id = p.id_usuario
            left join public.perfis pf on pf.id_usuario = p.id_usuario
            where p.id_usuario = @userId
            order by coalesce(p.paid_at, p.criado_em) desc
            limit 12
            """;
        Add(command, "userId", userId, NpgsqlDbType.Uuid);

        var rows = new List<AdminPaymentDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new AdminPaymentDto(reader.GetGuid(0), reader.GetGuid(1), ReadNullableString(reader, 2), ReadNullableString(reader, 3), reader.GetString(4), ReadNullableString(reader, 5), ReadNullableString(reader, 6), ReadNullableString(reader, 7), ReadNullableDecimal(reader, 8), ReadNullableString(reader, 9), ReadNullableDateTimeOffset(reader, 10), reader.GetFieldValue<DateTimeOffset>(11)));
        }
        return rows;
    }

    private static async Task<IReadOnlyList<AdminUserActionLogDto>> ListAdminUserActionLogsAsync(NpgsqlConnection connection, Guid targetUserId)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, admin_user_id, admin_email, target_user_id, action, reason, created_at
            from public.admin_user_actions
            where target_user_id = @targetUserId
            order by created_at desc
            limit 20
            """;
        Add(command, "targetUserId", targetUserId, NpgsqlDbType.Uuid);

        var rows = new List<AdminUserActionLogDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new AdminUserActionLogDto(reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2), reader.GetGuid(3), reader.GetString(4), ReadNullableString(reader, 5), reader.GetFieldValue<DateTimeOffset>(6)));
        }
        return rows;
    }

    private static async Task InsertAdminUserActionLogAsync(NpgsqlConnection connection, Guid adminUserId, string adminEmail, Guid targetUserId, string action, string? reason, object? metadata)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.admin_user_actions (admin_user_id, admin_email, target_user_id, action, reason, metadata)
            values (@adminUserId, @adminEmail, @targetUserId, @action, @reason, @metadata::jsonb)
            """;
        Add(command, "adminUserId", adminUserId, NpgsqlDbType.Uuid);
        Add(command, "adminEmail", adminEmail, NpgsqlDbType.Text);
        Add(command, "targetUserId", targetUserId, NpgsqlDbType.Uuid);
        Add(command, "action", action, NpgsqlDbType.Text);
        Add(command, "reason", NullIfWhiteSpace(reason), NpgsqlDbType.Text);
        Add(command, "metadata", JsonSerializer.Serialize(metadata ?? new { }), NpgsqlDbType.Text);
        await command.ExecuteNonQueryAsync();
    }

    private static PagedResultDto<T> BuildPagedResult<T>(IReadOnlyList<T> items, int total, int page, int pageSize)
    {
        var totalPages = Math.Max(1, (int)Math.Ceiling(total / (double)pageSize));
        return new PagedResultDto<T>(items, total, page, pageSize, totalPages);
    }

    private static string? ReadNullableString(NpgsqlDataReader reader, int index)
        => reader.IsDBNull(index) ? null : reader.GetString(index);

    private static decimal? ReadNullableDecimal(NpgsqlDataReader reader, int index)
        => reader.IsDBNull(index) ? null : reader.GetDecimal(index);

    private static DateTimeOffset? ReadNullableDateTimeOffset(NpgsqlDataReader reader, int index)
        => reader.IsDBNull(index) ? null : reader.GetFieldValue<DateTimeOffset>(index);
}
