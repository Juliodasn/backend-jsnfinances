using System.Text.Json;
using JsnFinances.Api.Domain;
using Npgsql;
using NpgsqlTypes;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    private static readonly string[] AllowedCardBrands = ["Visa", "Mastercard", "Elo", "Amex", "Hipercard", "Outro"];

    private async Task EnsureCreditCardsSchemaAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandTimeout = 90;
        command.CommandText = """
            create table if not exists public.credit_cards (
                id text not null,
                id_usuario uuid not null references auth.users(id) on delete cascade,
                nome text not null,
                bandeira text not null default 'Outro',
                limite numeric(14,2) not null default 0,
                dia_fechamento int not null default 10,
                dia_vencimento int not null default 15,
                cor text not null default '#4f46e5',
                portadores text[] not null default '{}'::text[],
                pagamento_automatico boolean not null default true,
                id_conta_pagamento uuid null,
                conta_pagamento_nome text null,
                ativo boolean not null default true,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now()),
                constraint pk_credit_cards primary key (id_usuario, id),
                constraint ck_credit_cards_dia_fechamento check (dia_fechamento between 1 and 31),
                constraint ck_credit_cards_dia_vencimento check (dia_vencimento between 1 and 31),
                constraint ck_credit_cards_limite check (limite >= 0)
            );

            create table if not exists public.credit_card_purchases (
                id text not null,
                id_usuario uuid not null references auth.users(id) on delete cascade,
                id_cartao text not null,
                descricao text not null,
                categoria text not null default 'Geral',
                id_subcategoria uuid null,
                subcategoria_nome text null,
                data_compra date not null,
                valor numeric(14,2) not null,
                parcelas int not null default 1,
                observacao text null,
                portador text null,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now()),
                constraint pk_credit_card_purchases primary key (id_usuario, id),
                constraint fk_credit_card_purchases_card foreign key (id_usuario, id_cartao) references public.credit_cards(id_usuario, id) on delete cascade,
                constraint ck_credit_card_purchases_valor check (valor > 0),
                constraint ck_credit_card_purchases_parcelas check (parcelas between 1 and 60)
            );

            create table if not exists public.credit_card_paid_invoices (
                id_usuario uuid not null references auth.users(id) on delete cascade,
                id_cartao text not null,
                fatura_key text not null,
                pago_em timestamptz not null default timezone('utc', now()),
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now()),
                constraint pk_credit_card_paid_invoices primary key (id_usuario, id_cartao, fatura_key),
                constraint fk_credit_card_paid_invoices_card foreign key (id_usuario, id_cartao) references public.credit_cards(id_usuario, id) on delete cascade
            );

            create table if not exists public.credit_card_recurring_purchases (
                id text not null,
                id_usuario uuid not null references auth.users(id) on delete cascade,
                id_cartao text not null,
                descricao text not null,
                categoria text not null default 'Assinaturas',
                id_subcategoria uuid null,
                subcategoria_nome text null,
                valor numeric(14,2) not null,
                parcelas int not null default 1,
                frequencia text not null default 'mensal',
                inicio date not null,
                fim date null,
                observacao text null,
                portador text null,
                ativo boolean not null default true,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now()),
                constraint pk_credit_card_recurring_purchases primary key (id_usuario, id),
                constraint fk_credit_card_recurring_purchases_card foreign key (id_usuario, id_cartao) references public.credit_cards(id_usuario, id) on delete cascade,
                constraint ck_credit_card_recurring_valor check (valor > 0),
                constraint ck_credit_card_recurring_parcelas check (parcelas between 1 and 60),
                constraint ck_credit_card_recurring_frequencia check (frequencia in ('mensal', 'anual'))
            );

            create table if not exists public.credit_card_payment_records (
                id text not null,
                id_usuario uuid not null references auth.users(id) on delete cascade,
                id_cartao text not null,
                fatura_key text not null,
                valor_pago numeric(14,2) not null,
                pago_em date not null,
                id_conta uuid null,
                conta_nome text null,
                id_saida_pagamento uuid null,
                automatico boolean not null default false,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now()),
                constraint pk_credit_card_payment_records primary key (id_usuario, id),
                constraint fk_credit_card_payment_records_card foreign key (id_usuario, id_cartao) references public.credit_cards(id_usuario, id) on delete cascade,
                constraint fk_credit_card_payment_records_account foreign key (id_conta) references public.contas(id) on delete set null,
                constraint ck_credit_card_payment_records_valor check (valor_pago > 0)
            );

            alter table public.credit_cards add column if not exists pagamento_automatico boolean not null default true;
            alter table public.credit_cards add column if not exists id_conta_pagamento uuid null;
            alter table public.credit_cards add column if not exists conta_pagamento_nome text null;
            alter table public.credit_card_purchases add column if not exists id_subcategoria uuid null;
            alter table public.credit_card_purchases add column if not exists subcategoria_nome text null;
            alter table public.credit_card_recurring_purchases add column if not exists id_subcategoria uuid null;
            alter table public.credit_card_recurring_purchases add column if not exists subcategoria_nome text null;
            alter table public.credit_card_payment_records add column if not exists automatico boolean not null default false;

            do $$
            begin
                if not exists (
                    select 1 from pg_constraint where conname = 'fk_credit_cards_payment_account'
                ) then
                    alter table public.credit_cards
                    add constraint fk_credit_cards_payment_account
                    foreign key (id_conta_pagamento) references public.contas(id) on delete set null;
                end if;
            end $$;

            create index if not exists ix_credit_cards_user_active on public.credit_cards (id_usuario, ativo);
            create index if not exists ix_credit_card_purchases_user_card_date on public.credit_card_purchases (id_usuario, id_cartao, data_compra desc);
            create index if not exists ix_credit_card_purchases_user_date on public.credit_card_purchases (id_usuario, data_compra desc);
            create index if not exists ix_credit_card_paid_invoices_user_card on public.credit_card_paid_invoices (id_usuario, id_cartao, fatura_key);
            create index if not exists ix_credit_card_recurring_user_card on public.credit_card_recurring_purchases (id_usuario, id_cartao, ativo);
            create index if not exists ix_credit_card_payment_records_user_card on public.credit_card_payment_records (id_usuario, id_cartao, fatura_key);
            """;
        await command.ExecuteNonQueryAsync();
    }

    public async Task<CreditCardsSnapshotDto> GetCreditCardsSnapshotAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();

        var cards = new List<CreditCardDto>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                select id, nome, bandeira, limite, dia_fechamento, dia_vencimento, cor, portadores, pagamento_automatico, id_conta_pagamento, conta_pagamento_nome, ativo, criado_em
                from public.credit_cards
                where id_usuario = @userId
                order by criado_em desc, nome asc
                """;
            Add(command, "userId", userId);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                cards.Add(new CreditCardDto(
                    reader.GetString(0),
                    reader.GetString(1),
                    NormalizeCardBrand(reader.GetString(2)),
                    reader.GetDecimal(3),
                    reader.GetInt32(4),
                    reader.GetInt32(5),
                    reader.GetString(6),
                    reader.IsDBNull(7) ? [] : reader.GetFieldValue<string[]>(7),
                    reader.GetBoolean(8),
                    reader.IsDBNull(9) ? null : reader.GetGuid(9),
                    ReadNullableString(reader, 10),
                    reader.GetBoolean(11),
                    ToIsoString(reader.GetFieldValue<DateTimeOffset>(12))));
            }
        }

        var purchases = new List<CreditCardPurchaseDto>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                select id, id_cartao, descricao, categoria, id_subcategoria, subcategoria_nome, data_compra, valor, parcelas, observacao, portador, criado_em
                from public.credit_card_purchases
                where id_usuario = @userId
                order by data_compra desc, criado_em desc
                """;
            Add(command, "userId", userId);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                purchases.Add(new CreditCardPurchaseDto(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetGuid(4),
                    ReadNullableString(reader, 5),
                    ToDateString(reader.GetFieldValue<DateOnly>(6)),
                    reader.GetDecimal(7),
                    reader.GetInt32(8),
                    ReadNullableString(reader, 9),
                    ReadNullableString(reader, 10),
                    ToIsoString(reader.GetFieldValue<DateTimeOffset>(11))));
            }
        }

        var paidInvoices = new List<CreditCardPaidInvoiceDto>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                select id_cartao, fatura_key, pago_em
                from public.credit_card_paid_invoices
                where id_usuario = @userId
                order by fatura_key desc
                """;
            Add(command, "userId", userId);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                paidInvoices.Add(new CreditCardPaidInvoiceDto(
                    reader.GetString(0),
                    reader.GetString(1),
                    ToIsoString(reader.GetFieldValue<DateTimeOffset>(2))));
            }
        }

        var recurringRules = new List<CreditCardRecurringPurchaseDto>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                select id, id_cartao, descricao, categoria, id_subcategoria, subcategoria_nome, valor, parcelas, frequencia, inicio, fim, observacao, portador, ativo
                from public.credit_card_recurring_purchases
                where id_usuario = @userId
                order by inicio desc, criado_em desc
                """;
            Add(command, "userId", userId);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                recurringRules.Add(new CreditCardRecurringPurchaseDto(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetGuid(4),
                    ReadNullableString(reader, 5),
                    reader.GetDecimal(6),
                    reader.GetInt32(7),
                    reader.GetString(8) == "anual" ? "anual" : "mensal",
                    ToDateString(reader.GetFieldValue<DateOnly>(9)),
                    reader.IsDBNull(10) ? null : ToDateString(reader.GetFieldValue<DateOnly>(10)),
                    ReadNullableString(reader, 11),
                    ReadNullableString(reader, 12),
                    reader.GetBoolean(13)));
            }
        }

        var paymentRecords = new List<CreditCardPaymentRecordDto>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                select id, id_cartao, fatura_key, valor_pago, pago_em, id_conta, conta_nome, id_saida_pagamento, automatico
                from public.credit_card_payment_records
                where id_usuario = @userId
                order by pago_em desc, criado_em desc
                """;
            Add(command, "userId", userId);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                paymentRecords.Add(new CreditCardPaymentRecordDto(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetDecimal(3),
                    ToDateString(reader.GetFieldValue<DateOnly>(4)),
                    reader.IsDBNull(5) ? null : reader.GetGuid(5),
                    ReadNullableString(reader, 6),
                    reader.IsDBNull(7) ? null : reader.GetGuid(7),
                    reader.GetBoolean(8)));
            }
        }

        return new CreditCardsSnapshotDto(cards, purchases, paidInvoices, recurringRules, paymentRecords, DateTimeOffset.UtcNow);
    }

    public async Task SaveCreditCardsSnapshotAsync(Guid userId, CreditCardsSnapshotRequest request)
    {
        // O formato anterior executava um DELETE e depois um INSERT por registro.
        // Como o backend está separado do PostgreSQL, cada ExecuteNonQueryAsync
        // gerava um round-trip. O tempo crescia junto com o histórico do cartão.
        // Agora todo o snapshot é persistido em uma única operação set-based.
        var cardsJson = JsonSerializer.Serialize(request.Cards ?? []);
        var purchasesJson = JsonSerializer.Serialize(request.Purchases ?? []);
        var paidInvoicesJson = JsonSerializer.Serialize(request.PaidInvoices ?? []);
        var recurringRulesJson = JsonSerializer.Serialize(request.RecurringRules ?? []);
        var paymentRecordsJson = JsonSerializer.Serialize(request.PaymentRecords ?? []);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = 90;
        command.CommandText = """
            delete from public.credit_card_payment_records where id_usuario = @userId;
            delete from public.credit_card_paid_invoices where id_usuario = @userId;
            delete from public.credit_card_recurring_purchases where id_usuario = @userId;
            delete from public.credit_card_purchases where id_usuario = @userId;
            delete from public.credit_cards where id_usuario = @userId;

            insert into public.credit_cards
                (id, id_usuario, nome, bandeira, limite, dia_fechamento, dia_vencimento, cor, portadores,
                 pagamento_automatico, id_conta_pagamento, conta_pagamento_nome, ativo, criado_em, atualizado_em)
            select
                btrim(item ->> 'id'),
                @userId,
                btrim(item ->> 'nome'),
                case
                    when lower(coalesce(item ->> 'bandeira', '')) = 'visa' then 'Visa'
                    when lower(coalesce(item ->> 'bandeira', '')) = 'mastercard' then 'Mastercard'
                    when lower(coalesce(item ->> 'bandeira', '')) = 'elo' then 'Elo'
                    when lower(coalesce(item ->> 'bandeira', '')) = 'amex' then 'Amex'
                    when lower(coalesce(item ->> 'bandeira', '')) = 'hipercard' then 'Hipercard'
                    else 'Outro'
                end,
                greatest(coalesce(nullif(item ->> 'limite', '')::numeric, 0), 0),
                least(31, greatest(1, coalesce(nullif(item ->> 'diaFechamento', '')::int, 1))),
                least(31, greatest(1, coalesce(nullif(item ->> 'diaVencimento', '')::int, 1))),
                coalesce(nullif(btrim(item ->> 'cor'), ''), '#4f46e5'),
                array(select distinct btrim(holder.value) from jsonb_array_elements_text(coalesce(item -> 'portadores', '[]'::jsonb)) as holder(value) where btrim(holder.value) <> ''),
                coalesce(nullif(item ->> 'pagamentoAutomatico', '')::boolean, true),
                nullif(item ->> 'idContaPagamento', '')::uuid,
                nullif(btrim(item ->> 'contaPagamentoNome'), ''),
                coalesce(nullif(item ->> 'ativo', '')::boolean, true),
                coalesce(nullif(item ->> 'criadoEm', '')::timestamptz, now()),
                now()
            from jsonb_array_elements(@cardsJson) item
            where nullif(btrim(item ->> 'id'), '') is not null
              and nullif(btrim(item ->> 'nome'), '') is not null
            on conflict do nothing;

            insert into public.credit_card_purchases
                (id, id_usuario, id_cartao, descricao, categoria, id_subcategoria, subcategoria_nome, data_compra, valor, parcelas,
                 observacao, portador, criado_em, atualizado_em)
            select
                btrim(item ->> 'id'),
                @userId,
                btrim(item ->> 'idCartao'),
                btrim(item ->> 'descricao'),
                coalesce(nullif(btrim(item ->> 'categoria'), ''), 'Geral'),
                nullif(item ->> 'idSubcategoria', '')::uuid,
                nullif(btrim(item ->> 'subcategoriaNome'), ''),
                coalesce(nullif(item ->> 'dataCompra', '')::date, current_date),
                coalesce(nullif(item ->> 'valor', '')::numeric, 0),
                least(60, greatest(1, coalesce(nullif(item ->> 'parcelas', '')::int, 1))),
                nullif(btrim(item ->> 'observacao'), ''),
                nullif(btrim(item ->> 'portador'), ''),
                coalesce(nullif(item ->> 'criadoEm', '')::timestamptz, now()),
                now()
            from jsonb_array_elements(@purchasesJson) item
            where nullif(btrim(item ->> 'id'), '') is not null
              and nullif(btrim(item ->> 'idCartao'), '') is not null
              and nullif(btrim(item ->> 'descricao'), '') is not null
              and coalesce(nullif(item ->> 'valor', '')::numeric, 0) > 0
            on conflict do nothing;

            insert into public.credit_card_paid_invoices
                (id_usuario, id_cartao, fatura_key, pago_em, criado_em, atualizado_em)
            select
                @userId,
                btrim(item ->> 'idCartao'),
                btrim(item ->> 'faturaKey'),
                coalesce(nullif(item ->> 'pagoEm', '')::timestamptz, now()),
                now(),
                now()
            from jsonb_array_elements(@paidInvoicesJson) item
            where nullif(btrim(item ->> 'idCartao'), '') is not null
              and nullif(btrim(item ->> 'faturaKey'), '') is not null
            on conflict do nothing;

            insert into public.credit_card_recurring_purchases
                (id, id_usuario, id_cartao, descricao, categoria, id_subcategoria, subcategoria_nome, valor, parcelas, frequencia,
                 inicio, fim, observacao, portador, ativo, criado_em, atualizado_em)
            select
                btrim(item ->> 'id'),
                @userId,
                btrim(item ->> 'idCartao'),
                btrim(item ->> 'descricao'),
                coalesce(nullif(btrim(item ->> 'categoria'), ''), 'Assinaturas'),
                nullif(item ->> 'idSubcategoria', '')::uuid,
                nullif(btrim(item ->> 'subcategoriaNome'), ''),
                coalesce(nullif(item ->> 'valor', '')::numeric, 0),
                least(60, greatest(1, coalesce(nullif(item ->> 'parcelas', '')::int, 1))),
                case when lower(coalesce(item ->> 'frequencia', '')) = 'anual' then 'anual' else 'mensal' end,
                coalesce(nullif(item ->> 'inicio', '')::date, current_date),
                nullif(item ->> 'fim', '')::date,
                nullif(btrim(item ->> 'observacao'), ''),
                nullif(btrim(item ->> 'portador'), ''),
                coalesce(nullif(item ->> 'ativo', '')::boolean, true),
                now(),
                now()
            from jsonb_array_elements(@recurringRulesJson) item
            where nullif(btrim(item ->> 'id'), '') is not null
              and nullif(btrim(item ->> 'idCartao'), '') is not null
              and nullif(btrim(item ->> 'descricao'), '') is not null
              and coalesce(nullif(item ->> 'valor', '')::numeric, 0) > 0
            on conflict do nothing;

            insert into public.credit_card_payment_records
                (id, id_usuario, id_cartao, fatura_key, valor_pago, pago_em, id_conta,
                 conta_nome, id_saida_pagamento, automatico, criado_em, atualizado_em)
            select
                btrim(item ->> 'id'),
                @userId,
                btrim(item ->> 'idCartao'),
                btrim(item ->> 'faturaKey'),
                coalesce(nullif(item ->> 'valorPago', '')::numeric, 0),
                coalesce(nullif(item ->> 'pagoEm', '')::date, current_date),
                nullif(item ->> 'idConta', '')::uuid,
                nullif(btrim(item ->> 'contaNome'), ''),
                nullif(item ->> 'idSaidaPagamento', '')::uuid,
                coalesce(nullif(item ->> 'automatico', '')::boolean, false),
                now(),
                now()
            from jsonb_array_elements(@paymentRecordsJson) item
            where nullif(btrim(item ->> 'id'), '') is not null
              and nullif(btrim(item ->> 'idCartao'), '') is not null
              and nullif(btrim(item ->> 'faturaKey'), '') is not null
              and coalesce(nullif(item ->> 'valorPago', '')::numeric, 0) > 0
            on conflict do nothing;
            """;

        Add(command, "userId", userId, NpgsqlDbType.Uuid);
        Add(command, "cardsJson", cardsJson, NpgsqlDbType.Jsonb);
        Add(command, "purchasesJson", purchasesJson, NpgsqlDbType.Jsonb);
        Add(command, "paidInvoicesJson", paidInvoicesJson, NpgsqlDbType.Jsonb);
        Add(command, "recurringRulesJson", recurringRulesJson, NpgsqlDbType.Jsonb);
        Add(command, "paymentRecordsJson", paymentRecordsJson, NpgsqlDbType.Jsonb);

        try
        {
            await command.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    private static async Task DeleteCreditCardsSnapshotAsync(NpgsqlConnection connection, Guid userId)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            delete from public.credit_card_payment_records where id_usuario = @userId;
            delete from public.credit_card_paid_invoices where id_usuario = @userId;
            delete from public.credit_card_recurring_purchases where id_usuario = @userId;
            delete from public.credit_card_purchases where id_usuario = @userId;
            delete from public.credit_cards where id_usuario = @userId;
            """;
        Add(command, "userId", userId);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task InsertCreditCardsAsync(NpgsqlConnection connection, Guid userId, IReadOnlyList<CreditCardRequest> cards)
    {
        foreach (var card in cards.Where(c => !string.IsNullOrWhiteSpace(c.Id) && !string.IsNullOrWhiteSpace(c.Nome)))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                insert into public.credit_cards
                    (id, id_usuario, nome, bandeira, limite, dia_fechamento, dia_vencimento, cor, portadores, pagamento_automatico, id_conta_pagamento, conta_pagamento_nome, ativo, criado_em, atualizado_em)
                values
                    (@id, @userId, @nome, @bandeira, @limite, @diaFechamento, @diaVencimento, @cor, @portadores, @pagamentoAutomatico, @idContaPagamento, @contaPagamentoNome, @ativo, @criadoEm, timezone('utc', now()))
                """;
            Add(command, "id", card.Id.Trim(), NpgsqlDbType.Text);
            Add(command, "userId", userId, NpgsqlDbType.Uuid);
            Add(command, "nome", card.Nome.Trim(), NpgsqlDbType.Text);
            Add(command, "bandeira", NormalizeCardBrand(card.Bandeira), NpgsqlDbType.Text);
            Add(command, "limite", Math.Max(0, card.Limite), NpgsqlDbType.Numeric);
            Add(command, "diaFechamento", ClampDay(card.DiaFechamento), NpgsqlDbType.Integer);
            Add(command, "diaVencimento", ClampDay(card.DiaVencimento), NpgsqlDbType.Integer);
            Add(command, "cor", NullIfWhiteSpace(card.Cor) ?? "#4f46e5", NpgsqlDbType.Text);
            Add(command, "portadores", NormalizeTextArray(card.Portadores), NpgsqlDbType.Array | NpgsqlDbType.Text);
            Add(command, "pagamentoAutomatico", card.PagamentoAutomatico ?? true, NpgsqlDbType.Boolean);
            Add(command, "idContaPagamento", card.IdContaPagamento, NpgsqlDbType.Uuid);
            Add(command, "contaPagamentoNome", NullIfWhiteSpace(card.ContaPagamentoNome), NpgsqlDbType.Text);
            Add(command, "ativo", card.Ativo, NpgsqlDbType.Boolean);
            Add(command, "criadoEm", ParseDateTimeOffset(card.CriadoEm) ?? DateTimeOffset.UtcNow, NpgsqlDbType.TimestampTz);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task InsertCreditCardPurchasesAsync(NpgsqlConnection connection, Guid userId, IReadOnlyList<CreditCardPurchaseRequest> purchases)
    {
        foreach (var purchase in purchases.Where(p => !string.IsNullOrWhiteSpace(p.Id) && !string.IsNullOrWhiteSpace(p.IdCartao) && !string.IsNullOrWhiteSpace(p.Descricao)))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                insert into public.credit_card_purchases
                    (id, id_usuario, id_cartao, descricao, categoria, id_subcategoria, subcategoria_nome, data_compra, valor, parcelas, observacao, portador, criado_em, atualizado_em)
                values
                    (@id, @userId, @idCartao, @descricao, @categoria, @idSubcategoria, @subcategoriaNome, @dataCompra, @valor, @parcelas, @observacao, @portador, @criadoEm, timezone('utc', now()))
                on conflict do nothing
                """;
            Add(command, "id", purchase.Id.Trim(), NpgsqlDbType.Text);
            Add(command, "userId", userId, NpgsqlDbType.Uuid);
            Add(command, "idCartao", purchase.IdCartao.Trim(), NpgsqlDbType.Text);
            Add(command, "descricao", purchase.Descricao.Trim(), NpgsqlDbType.Text);
            Add(command, "categoria", NullIfWhiteSpace(purchase.Categoria) ?? "Geral", NpgsqlDbType.Text);
            Add(command, "idSubcategoria", purchase.IdSubcategoria, NpgsqlDbType.Uuid);
            Add(command, "subcategoriaNome", NullIfWhiteSpace(purchase.SubcategoriaNome), NpgsqlDbType.Text);
            Add(command, "dataCompra", ParseDateOnly(purchase.DataCompra) ?? DateOnly.FromDateTime(DateTime.UtcNow), NpgsqlDbType.Date);
            Add(command, "valor", purchase.Valor, NpgsqlDbType.Numeric);
            Add(command, "parcelas", Math.Clamp(purchase.Parcelas, 1, 60), NpgsqlDbType.Integer);
            Add(command, "observacao", NullIfWhiteSpace(purchase.Observacao), NpgsqlDbType.Text);
            Add(command, "portador", NullIfWhiteSpace(purchase.Portador), NpgsqlDbType.Text);
            Add(command, "criadoEm", ParseDateTimeOffset(purchase.CriadoEm) ?? DateTimeOffset.UtcNow, NpgsqlDbType.TimestampTz);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task InsertCreditCardPaidInvoicesAsync(NpgsqlConnection connection, Guid userId, IReadOnlyList<CreditCardPaidInvoiceRequest> paidInvoices)
    {
        foreach (var invoice in paidInvoices.Where(i => !string.IsNullOrWhiteSpace(i.IdCartao) && !string.IsNullOrWhiteSpace(i.FaturaKey)))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                insert into public.credit_card_paid_invoices
                    (id_usuario, id_cartao, fatura_key, pago_em, atualizado_em)
                values
                    (@userId, @idCartao, @faturaKey, @pagoEm, timezone('utc', now()))
                on conflict do nothing
                """;
            Add(command, "userId", userId, NpgsqlDbType.Uuid);
            Add(command, "idCartao", invoice.IdCartao.Trim(), NpgsqlDbType.Text);
            Add(command, "faturaKey", invoice.FaturaKey.Trim(), NpgsqlDbType.Text);
            Add(command, "pagoEm", ParseDateTimeOffset(invoice.PagoEm) ?? DateTimeOffset.UtcNow, NpgsqlDbType.TimestampTz);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task InsertCreditCardRecurringRulesAsync(NpgsqlConnection connection, Guid userId, IReadOnlyList<CreditCardRecurringPurchaseRequest> rules)
    {
        foreach (var rule in rules.Where(r => !string.IsNullOrWhiteSpace(r.Id) && !string.IsNullOrWhiteSpace(r.IdCartao) && !string.IsNullOrWhiteSpace(r.Descricao)))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                insert into public.credit_card_recurring_purchases
                    (id, id_usuario, id_cartao, descricao, categoria, id_subcategoria, subcategoria_nome, valor, parcelas, frequencia, inicio, fim, observacao, portador, ativo, atualizado_em)
                values
                    (@id, @userId, @idCartao, @descricao, @categoria, @idSubcategoria, @subcategoriaNome, @valor, @parcelas, @frequencia, @inicio, @fim, @observacao, @portador, @ativo, timezone('utc', now()))
                on conflict do nothing
                """;
            Add(command, "id", rule.Id.Trim(), NpgsqlDbType.Text);
            Add(command, "userId", userId, NpgsqlDbType.Uuid);
            Add(command, "idCartao", rule.IdCartao.Trim(), NpgsqlDbType.Text);
            Add(command, "descricao", rule.Descricao.Trim(), NpgsqlDbType.Text);
            Add(command, "categoria", NullIfWhiteSpace(rule.Categoria) ?? "Assinaturas", NpgsqlDbType.Text);
            Add(command, "idSubcategoria", rule.IdSubcategoria, NpgsqlDbType.Uuid);
            Add(command, "subcategoriaNome", NullIfWhiteSpace(rule.SubcategoriaNome), NpgsqlDbType.Text);
            Add(command, "valor", rule.Valor, NpgsqlDbType.Numeric);
            Add(command, "parcelas", Math.Clamp(rule.Parcelas, 1, 60), NpgsqlDbType.Integer);
            Add(command, "frequencia", rule.Frequencia == "anual" ? "anual" : "mensal", NpgsqlDbType.Text);
            Add(command, "inicio", ParseDateOnly(rule.Inicio) ?? DateOnly.FromDateTime(DateTime.UtcNow), NpgsqlDbType.Date);
            Add(command, "fim", ParseDateOnly(rule.Fim), NpgsqlDbType.Date);
            Add(command, "observacao", NullIfWhiteSpace(rule.Observacao), NpgsqlDbType.Text);
            Add(command, "portador", NullIfWhiteSpace(rule.Portador), NpgsqlDbType.Text);
            Add(command, "ativo", rule.Ativo, NpgsqlDbType.Boolean);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task InsertCreditCardPaymentRecordsAsync(NpgsqlConnection connection, Guid userId, IReadOnlyList<CreditCardPaymentRecordRequest> records)
    {
        foreach (var record in records.Where(r => !string.IsNullOrWhiteSpace(r.Id) && !string.IsNullOrWhiteSpace(r.IdCartao) && !string.IsNullOrWhiteSpace(r.FaturaKey)))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                insert into public.credit_card_payment_records
                    (id, id_usuario, id_cartao, fatura_key, valor_pago, pago_em, id_conta, conta_nome, id_saida_pagamento, automatico, atualizado_em)
                values
                    (@id, @userId, @idCartao, @faturaKey, @valorPago, @pagoEm, @idConta, @contaNome, @idSaidaPagamento, @automatico, timezone('utc', now()))
                on conflict do nothing
                """;
            Add(command, "id", record.Id.Trim(), NpgsqlDbType.Text);
            Add(command, "userId", userId, NpgsqlDbType.Uuid);
            Add(command, "idCartao", record.IdCartao.Trim(), NpgsqlDbType.Text);
            Add(command, "faturaKey", record.FaturaKey.Trim(), NpgsqlDbType.Text);
            Add(command, "valorPago", record.ValorPago, NpgsqlDbType.Numeric);
            Add(command, "pagoEm", ParseDateOnly(record.PagoEm) ?? DateOnly.FromDateTime(DateTime.UtcNow), NpgsqlDbType.Date);
            Add(command, "idConta", record.IdConta, NpgsqlDbType.Uuid);
            Add(command, "contaNome", NullIfWhiteSpace(record.ContaNome), NpgsqlDbType.Text);
            Add(command, "idSaidaPagamento", record.IdSaidaPagamento, NpgsqlDbType.Uuid);
            Add(command, "automatico", record.Automatico ?? false, NpgsqlDbType.Boolean);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static string NormalizeCardBrand(string? brand)
    {
        var value = NullIfWhiteSpace(brand) ?? "Outro";
        return AllowedCardBrands.Contains(value, StringComparer.OrdinalIgnoreCase)
            ? AllowedCardBrands.First(item => item.Equals(value, StringComparison.OrdinalIgnoreCase))
            : "Outro";
    }

    private static int ClampDay(int value) => Math.Clamp(value <= 0 ? 1 : value, 1, 31);

    private static string[] NormalizeTextArray(string[]? values)
        => values is null ? [] : values.Select(NullIfWhiteSpace).Where(item => item is not null).Select(item => item!).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

    private static DateOnly? ParseDateOnly(string? value)
        => DateOnly.TryParse(value, out var date) ? date : null;

    private static DateTimeOffset? ParseDateTimeOffset(string? value)
        => DateTimeOffset.TryParse(value, out var date) ? date.ToUniversalTime() : null;

    private static string ToDateString(DateOnly date) => date.ToString("yyyy-MM-dd");

    private static string ToIsoString(DateTimeOffset date) => date.ToUniversalTime().ToString("O");
}
