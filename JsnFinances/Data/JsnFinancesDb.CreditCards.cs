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
                principal boolean not null default false,
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
                legado boolean not null default false,
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
            alter table public.credit_cards add column if not exists principal boolean not null default false;
            alter table public.credit_card_purchases add column if not exists id_subcategoria uuid null;
            alter table public.credit_card_purchases add column if not exists subcategoria_nome text null;
            alter table public.credit_card_recurring_purchases add column if not exists id_subcategoria uuid null;
            alter table public.credit_card_recurring_purchases add column if not exists subcategoria_nome text null;
            alter table public.credit_card_payment_records add column if not exists automatico boolean not null default false;
            alter table public.credit_card_payment_records add column if not exists legado boolean not null default false;

            create table if not exists public.credit_card_user_state (
                id_usuario uuid primary key references auth.users(id) on delete cascade,
                versao bigint not null default 0,
                atualizado_em timestamptz not null default timezone('utc', now())
            );

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

            with cartoes_ativos_ordenados as (
                select
                    id,
                    id_usuario,
                    row_number() over (
                        partition by id_usuario
                        order by principal desc, criado_em desc, nome asc, id asc
                    ) as posicao
                from public.credit_cards
                where ativo = true
            )
            update public.credit_cards cartao
            set principal = ordenado.posicao = 1
            from cartoes_ativos_ordenados ordenado
            where cartao.id = ordenado.id
              and cartao.id_usuario = ordenado.id_usuario
              and cartao.principal is distinct from (ordenado.posicao = 1);

            update public.credit_cards
            set principal = false
            where ativo = false and principal = true;

            create index if not exists ix_credit_cards_user_active on public.credit_cards (id_usuario, ativo);
            -- Vários cartões podem permanecer ativos; a unicidade vale somente para o cartão marcado como principal.
            create unique index if not exists ux_credit_cards_user_principal_active
                on public.credit_cards (id_usuario)
                where principal = true and ativo = true;
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

        long version;
        await using (var stateCommand = connection.CreateCommand())
        {
            stateCommand.CommandText = """
                insert into public.credit_card_user_state (id_usuario)
                values (@userId)
                on conflict (id_usuario) do nothing;

                select versao
                from public.credit_card_user_state
                where id_usuario = @userId;
                """;
            Add(stateCommand, "userId", userId, NpgsqlDbType.Uuid);
            version = Convert.ToInt64(await stateCommand.ExecuteScalarAsync() ?? 0L);
        }

        var cards = new List<CreditCardDto>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                select id, nome, bandeira, limite, dia_fechamento, dia_vencimento, cor, portadores, pagamento_automatico, id_conta_pagamento, conta_pagamento_nome, ativo, principal, criado_em
                from public.credit_cards
                where id_usuario = @userId
                order by principal desc, criado_em desc, nome asc
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
                    reader.GetBoolean(12),
                    ToIsoString(reader.GetFieldValue<DateTimeOffset>(13))));
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
                select id, id_cartao, fatura_key, valor_pago, pago_em, id_conta, conta_nome, id_saida_pagamento, automatico, legado
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
                    reader.GetBoolean(8),
                    reader.GetBoolean(9)));
            }
        }

        return new CreditCardsSnapshotDto(cards, purchases, paidInvoices, recurringRules, paymentRecords, DateTimeOffset.UtcNow, version);
    }

    public async Task<CreditCardsSaveResultDto> SaveCreditCardsSnapshotAsync(Guid userId, CreditCardsSnapshotRequest request)
    {
        // O formato anterior executava um DELETE e depois um INSERT por registro.
        // Como o backend está separado do PostgreSQL, cada ExecuteNonQueryAsync
        // gerava um round-trip. O tempo crescia junto com o histórico do cartão.
        // Agora todo o snapshot é persistido em uma única operação set-based.
        var normalizedCards = NormalizeCreditCardRequests(request.Cards ?? []);
        var cardsJson = JsonSerializer.Serialize(normalizedCards);
        var purchasesJson = JsonSerializer.Serialize(request.Purchases ?? []);
        var paidInvoicesJson = JsonSerializer.Serialize(request.PaidInvoices ?? []);
        var recurringRulesJson = JsonSerializer.Serialize(request.RecurringRules ?? []);
        var paymentRecordsJson = JsonSerializer.Serialize(request.PaymentRecords ?? []);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        long currentVersion;
        await using (var stateCommand = connection.CreateCommand())
        {
            stateCommand.Transaction = transaction;
            stateCommand.CommandText = """
                insert into public.credit_card_user_state (id_usuario)
                values (@userId)
                on conflict (id_usuario) do nothing;

                select versao
                from public.credit_card_user_state
                where id_usuario = @userId
                for update;
                """;
            Add(stateCommand, "userId", userId, NpgsqlDbType.Uuid);
            currentVersion = Convert.ToInt64(await stateCommand.ExecuteScalarAsync() ?? 0L);
        }

        // A versão é obrigatória depois que o módulo já recebeu a primeira gravação.
        // Isso também protege contra clientes antigos que ainda tentem enviar um
        // snapshot sem controle de concorrência.
        if ((!request.Version.HasValue && currentVersion > 0)
            || (request.Version.HasValue && request.Version.Value != currentVersion))
        {
            await transaction.RollbackAsync();
            throw new CreditCardsConcurrencyException(currentVersion);
        }

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = 90;
        command.CommandText = """
            -- O snapshot continua sendo o contrato de sincronização, mas a gravação
            -- não apaga mais todas as tabelas. Cada registro é atualizado por chave
            -- e somente itens removidos na versão válida são excluídos.
            update public.credit_cards
            set principal = false, atualizado_em = now()
            where id_usuario = @userId and principal = true;

            insert into public.credit_cards
                (id, id_usuario, nome, bandeira, limite, dia_fechamento, dia_vencimento, cor, portadores,
                 pagamento_automatico, id_conta_pagamento, conta_pagamento_nome, ativo, principal, criado_em, atualizado_em)
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
                coalesce(nullif(item ->> 'principal', '')::boolean, false),
                coalesce(nullif(item ->> 'criadoEm', '')::timestamptz, now()),
                now()
            from jsonb_array_elements(@cardsJson) item
            where nullif(btrim(item ->> 'id'), '') is not null
              and nullif(btrim(item ->> 'nome'), '') is not null
            on conflict (id_usuario, id) do update set
                nome = excluded.nome,
                bandeira = excluded.bandeira,
                limite = excluded.limite,
                dia_fechamento = excluded.dia_fechamento,
                dia_vencimento = excluded.dia_vencimento,
                cor = excluded.cor,
                portadores = excluded.portadores,
                pagamento_automatico = excluded.pagamento_automatico,
                id_conta_pagamento = excluded.id_conta_pagamento,
                conta_pagamento_nome = excluded.conta_pagamento_nome,
                ativo = excluded.ativo,
                principal = excluded.principal,
                atualizado_em = now();

            -- Um cartão com histórico não pode ser apagado por um cliente antigo ou
            -- por um snapshot incompleto. Ele deve ser arquivado na interface.
            delete from public.credit_cards card
            where card.id_usuario = @userId
              and not exists (
                  select 1 from jsonb_array_elements(@cardsJson) item
                  where btrim(item ->> 'id') = card.id
              )
              and not exists (
                  select 1 from public.credit_card_purchases purchase
                  where purchase.id_usuario = card.id_usuario and purchase.id_cartao = card.id
              )
              and not exists (
                  select 1 from public.credit_card_recurring_purchases recurring
                  where recurring.id_usuario = card.id_usuario and recurring.id_cartao = card.id
              )
              and not exists (
                  select 1 from public.credit_card_payment_records payment
                  where payment.id_usuario = card.id_usuario and payment.id_cartao = card.id
              )
              and not exists (
                  select 1 from public.credit_card_paid_invoices paid
                  where paid.id_usuario = card.id_usuario and paid.id_cartao = card.id
              );

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
            on conflict (id_usuario, id) do update set
                id_cartao = excluded.id_cartao,
                descricao = excluded.descricao,
                categoria = excluded.categoria,
                id_subcategoria = excluded.id_subcategoria,
                subcategoria_nome = excluded.subcategoria_nome,
                data_compra = excluded.data_compra,
                valor = excluded.valor,
                parcelas = excluded.parcelas,
                observacao = excluded.observacao,
                portador = excluded.portador,
                atualizado_em = now();

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
            on conflict (id_usuario, id_cartao, fatura_key) do update set
                pago_em = excluded.pago_em,
                atualizado_em = now();

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
            on conflict (id_usuario, id) do update set
                id_cartao = excluded.id_cartao,
                descricao = excluded.descricao,
                categoria = excluded.categoria,
                id_subcategoria = excluded.id_subcategoria,
                subcategoria_nome = excluded.subcategoria_nome,
                valor = excluded.valor,
                parcelas = excluded.parcelas,
                frequencia = excluded.frequencia,
                inicio = excluded.inicio,
                fim = excluded.fim,
                observacao = excluded.observacao,
                portador = excluded.portador,
                ativo = excluded.ativo,
                atualizado_em = now();

            insert into public.credit_card_payment_records
                (id, id_usuario, id_cartao, fatura_key, valor_pago, pago_em, id_conta,
                 conta_nome, id_saida_pagamento, automatico, legado, criado_em, atualizado_em)
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
                coalesce(nullif(item ->> 'legado', '')::boolean, false),
                now(),
                now()
            from jsonb_array_elements(@paymentRecordsJson) item
            where nullif(btrim(item ->> 'id'), '') is not null
              and nullif(btrim(item ->> 'idCartao'), '') is not null
              and nullif(btrim(item ->> 'faturaKey'), '') is not null
              and coalesce(nullif(item ->> 'valorPago', '')::numeric, 0) > 0
            on conflict (id_usuario, id) do update set
                id_cartao = excluded.id_cartao,
                fatura_key = excluded.fatura_key,
                valor_pago = excluded.valor_pago,
                pago_em = excluded.pago_em,
                id_conta = excluded.id_conta,
                conta_nome = excluded.conta_nome,
                id_saida_pagamento = excluded.id_saida_pagamento,
                automatico = excluded.automatico,
                legado = excluded.legado,
                atualizado_em = now();

            -- Registros de pagamento com saída bancária só podem ser removidos pelo
            -- endpoint transacional de desfazer pagamento. O snapshot nunca rompe o vínculo.
            delete from public.credit_card_payment_records payment
            where payment.id_usuario = @userId
              and payment.id_saida_pagamento is null
              and not exists (
                  select 1 from jsonb_array_elements(@paymentRecordsJson) item
                  where btrim(item ->> 'id') = payment.id
              );

            delete from public.credit_card_paid_invoices paid
            where paid.id_usuario = @userId
              and not exists (
                  select 1 from jsonb_array_elements(@paidInvoicesJson) item
                  where btrim(item ->> 'idCartao') = paid.id_cartao
                    and btrim(item ->> 'faturaKey') = paid.fatura_key
              );

            delete from public.credit_card_recurring_purchases recurring
            where recurring.id_usuario = @userId
              and not exists (
                  select 1 from jsonb_array_elements(@recurringRulesJson) item
                  where btrim(item ->> 'id') = recurring.id
              );

            delete from public.credit_card_purchases purchase
            where purchase.id_usuario = @userId
              and not exists (
                  select 1 from jsonb_array_elements(@purchasesJson) item
                  where btrim(item ->> 'id') = purchase.id
              );
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

            var nextVersion = currentVersion + 1;
            var updatedAt = DateTimeOffset.UtcNow;
            await using (var versionCommand = connection.CreateCommand())
            {
                versionCommand.Transaction = transaction;
                versionCommand.CommandText = """
                    update public.credit_card_user_state
                    set versao = @version, atualizado_em = @updatedAt
                    where id_usuario = @userId;
                    """;
                Add(versionCommand, "version", nextVersion, NpgsqlDbType.Bigint);
                Add(versionCommand, "updatedAt", updatedAt, NpgsqlDbType.TimestampTz);
                Add(versionCommand, "userId", userId, NpgsqlDbType.Uuid);
                await versionCommand.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
            return new CreditCardsSaveResultDto(nextVersion, updatedAt);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<CreditCardPaymentRegistrationResultDto> RegisterCreditCardInvoicePaymentAsync(
        Guid userId,
        RegisterCreditCardInvoicePaymentRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.IdPagamento)
            || string.IsNullOrWhiteSpace(request.IdCartao)
            || string.IsNullOrWhiteSpace(request.FaturaKey))
        {
            throw new ArgumentException("Pagamento, cartão e fatura são obrigatórios.");
        }

        if (request.ValorPago <= 0)
        {
            throw new ArgumentException("O valor pago deve ser maior que zero.");
        }

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            long currentVersion;
            await using (var stateCommand = connection.CreateCommand())
            {
                stateCommand.Transaction = transaction;
                stateCommand.CommandText = """
                    insert into public.credit_card_user_state (id_usuario)
                    values (@userId)
                    on conflict (id_usuario) do nothing;

                    select versao
                    from public.credit_card_user_state
                    where id_usuario = @userId
                    for update;
                    """;
                Add(stateCommand, "userId", userId, NpgsqlDbType.Uuid);
                currentVersion = Convert.ToInt64(await stateCommand.ExecuteScalarAsync() ?? 0L);
            }

            if ((!request.Version.HasValue && currentVersion > 0)
                || (request.Version.HasValue && request.Version.Value != currentVersion))
            {
                throw new CreditCardsConcurrencyException(currentVersion);
            }

            string accountName;
            await using (var accountCommand = connection.CreateCommand())
            {
                accountCommand.Transaction = transaction;
                accountCommand.CommandText = """
                    select nome
                    from public.contas
                    where id = @accountId
                      and id_usuario = @userId
                      and ativo = true;
                    """;
                Add(accountCommand, "accountId", request.IdConta, NpgsqlDbType.Uuid);
                Add(accountCommand, "userId", userId, NpgsqlDbType.Uuid);
                accountName = Convert.ToString(await accountCommand.ExecuteScalarAsync()) ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(accountName))
            {
                throw new InvalidOperationException("A conta selecionada não existe, está inativa ou pertence a outro usuário.");
            }

            await using (var cardCommand = connection.CreateCommand())
            {
                cardCommand.Transaction = transaction;
                cardCommand.CommandText = """
                    select exists (
                        select 1
                        from public.credit_cards
                        where id_usuario = @userId and id = @cardId
                    );
                    """;
                Add(cardCommand, "userId", userId, NpgsqlDbType.Uuid);
                Add(cardCommand, "cardId", request.IdCartao.Trim(), NpgsqlDbType.Text);
                if (!Convert.ToBoolean(await cardCommand.ExecuteScalarAsync() ?? false))
                {
                    throw new InvalidOperationException("O cartão informado não existe mais.");
                }
            }

            var paymentId = request.IdPagamento.Trim();
            var outputId = Guid.NewGuid();
            var description = string.IsNullOrWhiteSpace(request.Descricao)
                ? $"Pagamento de fatura {request.FaturaKey}"
                : request.Descricao.Trim();

            await using (var outputCommand = connection.CreateCommand())
            {
                outputCommand.Transaction = transaction;
                outputCommand.CommandText = """
                    insert into public.saidas
                        (id, id_usuario, descricao, categoria, valor, data_movimentacao, observacao, id_conta)
                    values
                        (@id, @userId, @description, 'Cartão de crédito', @value, @paymentDate, @notes, @accountId);
                    """;
                Add(outputCommand, "id", outputId, NpgsqlDbType.Uuid);
                Add(outputCommand, "userId", userId, NpgsqlDbType.Uuid);
                Add(outputCommand, "description", description, NpgsqlDbType.Text);
                Add(outputCommand, "value", request.ValorPago, NpgsqlDbType.Numeric);
                Add(outputCommand, "paymentDate", request.PagoEm, NpgsqlDbType.Date);
                Add(outputCommand, "notes", NullIfWhiteSpace(request.Observacao), NpgsqlDbType.Text);
                Add(outputCommand, "accountId", request.IdConta, NpgsqlDbType.Uuid);
                await outputCommand.ExecuteNonQueryAsync();
            }

            await using (var paymentCommand = connection.CreateCommand())
            {
                paymentCommand.Transaction = transaction;
                paymentCommand.CommandText = """
                    insert into public.credit_card_payment_records
                        (id, id_usuario, id_cartao, fatura_key, valor_pago, pago_em, id_conta,
                         conta_nome, id_saida_pagamento, automatico, legado, criado_em, atualizado_em)
                    values
                        (@id, @userId, @cardId, @invoiceKey, @value, @paymentDate, @accountId,
                         @accountName, @outputId, false, false, now(), now());
                    """;
                Add(paymentCommand, "id", paymentId, NpgsqlDbType.Text);
                Add(paymentCommand, "userId", userId, NpgsqlDbType.Uuid);
                Add(paymentCommand, "cardId", request.IdCartao.Trim(), NpgsqlDbType.Text);
                Add(paymentCommand, "invoiceKey", request.FaturaKey.Trim(), NpgsqlDbType.Text);
                Add(paymentCommand, "value", request.ValorPago, NpgsqlDbType.Numeric);
                Add(paymentCommand, "paymentDate", request.PagoEm, NpgsqlDbType.Date);
                Add(paymentCommand, "accountId", request.IdConta, NpgsqlDbType.Uuid);
                Add(paymentCommand, "accountName", accountName.Trim(), NpgsqlDbType.Text);
                Add(paymentCommand, "outputId", outputId, NpgsqlDbType.Uuid);
                await paymentCommand.ExecuteNonQueryAsync();
            }

            // O registro detalhado passa a ser a única fonte oficial. Qualquer marca
            // legada da mesma fatura é removida na mesma transação.
            await using (var legacyCommand = connection.CreateCommand())
            {
                legacyCommand.Transaction = transaction;
                legacyCommand.CommandText = """
                    delete from public.credit_card_paid_invoices
                    where id_usuario = @userId
                      and id_cartao = @cardId
                      and fatura_key = @invoiceKey;
                    """;
                Add(legacyCommand, "userId", userId, NpgsqlDbType.Uuid);
                Add(legacyCommand, "cardId", request.IdCartao.Trim(), NpgsqlDbType.Text);
                Add(legacyCommand, "invoiceKey", request.FaturaKey.Trim(), NpgsqlDbType.Text);
                await legacyCommand.ExecuteNonQueryAsync();
            }

            var nextVersion = currentVersion + 1;
            var updatedAt = DateTimeOffset.UtcNow;
            await using (var versionCommand = connection.CreateCommand())
            {
                versionCommand.Transaction = transaction;
                versionCommand.CommandText = """
                    update public.credit_card_user_state
                    set versao = @version, atualizado_em = @updatedAt
                    where id_usuario = @userId;
                    """;
                Add(versionCommand, "version", nextVersion, NpgsqlDbType.Bigint);
                Add(versionCommand, "updatedAt", updatedAt, NpgsqlDbType.TimestampTz);
                Add(versionCommand, "userId", userId, NpgsqlDbType.Uuid);
                await versionCommand.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
            var paymentRecord = new CreditCardPaymentRecordDto(
                paymentId,
                request.IdCartao.Trim(),
                request.FaturaKey.Trim(),
                request.ValorPago,
                ToDateString(request.PagoEm),
                request.IdConta,
                accountName.Trim(),
                outputId,
                false,
                false);
            return new CreditCardPaymentRegistrationResultDto(nextVersion, updatedAt, paymentRecord);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<CreditCardsSaveResultDto> UndoCreditCardInvoicePaymentsAsync(
        Guid userId,
        UndoCreditCardInvoicePaymentsRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.IdCartao) || string.IsNullOrWhiteSpace(request.FaturaKey))
        {
            throw new ArgumentException("Cartão e fatura são obrigatórios.");
        }

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            long currentVersion;
            await using (var stateCommand = connection.CreateCommand())
            {
                stateCommand.Transaction = transaction;
                stateCommand.CommandText = """
                    insert into public.credit_card_user_state (id_usuario)
                    values (@userId)
                    on conflict (id_usuario) do nothing;

                    select versao
                    from public.credit_card_user_state
                    where id_usuario = @userId
                    for update;
                    """;
                Add(stateCommand, "userId", userId, NpgsqlDbType.Uuid);
                currentVersion = Convert.ToInt64(await stateCommand.ExecuteScalarAsync() ?? 0L);
            }

            if ((!request.Version.HasValue && currentVersion > 0)
                || (request.Version.HasValue && request.Version.Value != currentVersion))
            {
                throw new CreditCardsConcurrencyException(currentVersion);
            }

            var targetPayments = new List<(string Id, Guid? SaidaId)>();
            await using (var paymentCommand = connection.CreateCommand())
            {
                paymentCommand.Transaction = transaction;
                paymentCommand.CommandText = """
                    select id, id_saida_pagamento
                    from public.credit_card_payment_records
                    where id_usuario = @userId
                      and id_cartao = @idCartao
                      and fatura_key = @faturaKey
                      and (@idPagamento is null or id = @idPagamento)
                    for update;
                    """;
                Add(paymentCommand, "userId", userId, NpgsqlDbType.Uuid);
                Add(paymentCommand, "idCartao", request.IdCartao.Trim(), NpgsqlDbType.Text);
                Add(paymentCommand, "faturaKey", request.FaturaKey.Trim(), NpgsqlDbType.Text);
                Add(paymentCommand, "idPagamento", NullIfWhiteSpace(request.IdPagamento), NpgsqlDbType.Text);

                await using var reader = await paymentCommand.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    targetPayments.Add((
                        reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetGuid(1)));
                }
            }

            if (!string.IsNullOrWhiteSpace(request.IdPagamento) && targetPayments.Count == 0)
            {
                throw new InvalidOperationException("O pagamento selecionado não existe mais ou já foi desfeito.");
            }

            // Cada pagamento real precisa remover exatamente a saída bancária que o
            // originou. Se o vínculo estiver inconsistente, a transação inteira falha:
            // o payment record permanece e nenhuma fatura é reaberta parcialmente.
            foreach (var target in targetPayments.Where(item => item.SaidaId.HasValue))
            {
                await using var outputCommand = connection.CreateCommand();
                outputCommand.Transaction = transaction;
                outputCommand.CommandText = """
                    delete from public.saidas
                    where id = @saidaId
                      and id_usuario = @userId;
                    """;
                Add(outputCommand, "saidaId", target.SaidaId!.Value, NpgsqlDbType.Uuid);
                Add(outputCommand, "userId", userId, NpgsqlDbType.Uuid);

                if (await outputCommand.ExecuteNonQueryAsync() != 1)
                {
                    throw new InvalidOperationException(
                        $"A saída bancária vinculada ao pagamento {target.Id} não foi encontrada. Nenhum pagamento foi desfeito.");
                }
            }

            await using (var deletePaymentsCommand = connection.CreateCommand())
            {
                deletePaymentsCommand.Transaction = transaction;
                deletePaymentsCommand.CommandText = """
                    delete from public.credit_card_payment_records
                    where id_usuario = @userId
                      and id_cartao = @idCartao
                      and fatura_key = @faturaKey
                      and (@idPagamento is null or id = @idPagamento);

                    delete from public.credit_card_paid_invoices
                    where id_usuario = @userId
                      and id_cartao = @idCartao
                      and fatura_key = @faturaKey
                      and not exists (
                          select 1
                          from public.credit_card_payment_records remaining
                          where remaining.id_usuario = @userId
                            and remaining.id_cartao = @idCartao
                            and remaining.fatura_key = @faturaKey
                      );
                    """;
                Add(deletePaymentsCommand, "userId", userId, NpgsqlDbType.Uuid);
                Add(deletePaymentsCommand, "idCartao", request.IdCartao.Trim(), NpgsqlDbType.Text);
                Add(deletePaymentsCommand, "faturaKey", request.FaturaKey.Trim(), NpgsqlDbType.Text);
                Add(deletePaymentsCommand, "idPagamento", NullIfWhiteSpace(request.IdPagamento), NpgsqlDbType.Text);
                await deletePaymentsCommand.ExecuteNonQueryAsync();
            }

            var nextVersion = currentVersion + 1;
            var updatedAt = DateTimeOffset.UtcNow;
            await using (var versionCommand = connection.CreateCommand())
            {
                versionCommand.Transaction = transaction;
                versionCommand.CommandText = """
                    update public.credit_card_user_state
                    set versao = @version, atualizado_em = @updatedAt
                    where id_usuario = @userId;
                    """;
                Add(versionCommand, "version", nextVersion, NpgsqlDbType.Bigint);
                Add(versionCommand, "updatedAt", updatedAt, NpgsqlDbType.TimestampTz);
                Add(versionCommand, "userId", userId, NpgsqlDbType.Uuid);
                await versionCommand.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
            return new CreditCardsSaveResultDto(nextVersion, updatedAt);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    private static IReadOnlyList<CreditCardRequest> NormalizeCreditCardRequests(IReadOnlyList<CreditCardRequest> cards)
    {
        var validCards = cards
            .Where(card => !string.IsNullOrWhiteSpace(card.Id) && !string.IsNullOrWhiteSpace(card.Nome))
            .ToList();

        var primaryCardId = validCards
            .FirstOrDefault(card => card.Ativo && card.Principal == true)?.Id
            ?? validCards.FirstOrDefault(card => card.Ativo)?.Id;

        return validCards
            .Select(card => card with
            {
                Principal = card.Ativo && !string.IsNullOrWhiteSpace(primaryCardId) && card.Id == primaryCardId
            })
            .ToList();
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
