using JsnFinances.Api.Domain;
using NpgsqlTypes;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    public async Task<IReadOnlyList<BoletoDto>> ListBoletosAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select b.id, b.descricao, b.beneficiario, b.categoria, b.id_subcategoria,
                   sc.nome as subcategoria_nome, b.tipo, b.valor_original, b.frequencia,
                   b.quantidade_parcelas, b.data_inicio, b.data_fim, b.id_conta_padrao,
                   c.nome as conta_padrao_nome, b.modo_pagamento, b.dias_lembrete,
                   b.codigo_barras, b.observacao, b.ativo
              from public.boletos b
              left join public.subcategorias sc on sc.id = b.id_subcategoria
              left join public.contas c on c.id = b.id_conta_padrao
             where b.id_usuario = @userId
             order by b.ativo desc, b.data_inicio desc, b.descricao
            """;
        Add(command, "userId", userId);
        var result = new List<BoletoDto>();
        await using var reader = await command.ExecuteReaderAsync();
        var headers = new List<(Guid Id,string Descricao,string? Beneficiario,string? Categoria,Guid? SubId,string? SubNome,string Tipo,decimal Valor,string Frequencia,int Qtd,DateOnly Inicio,DateOnly? Fim,Guid? ContaId,string? ContaNome,string Modo,int Dias,string? Codigo,string? Obs,bool Ativo)>();
        while (await reader.ReadAsync()) headers.Add((reader.GetGuid(0), reader.GetString(1), reader.IsDBNull(2)?null:reader.GetString(2), reader.IsDBNull(3)?null:reader.GetString(3), reader.IsDBNull(4)?null:reader.GetGuid(4), reader.IsDBNull(5)?null:reader.GetString(5), reader.GetString(6), reader.GetDecimal(7), reader.GetString(8), reader.GetInt32(9), reader.GetFieldValue<DateOnly>(10), reader.IsDBNull(11)?null:reader.GetFieldValue<DateOnly>(11), reader.IsDBNull(12)?null:reader.GetGuid(12), reader.IsDBNull(13)?null:reader.GetString(13), reader.GetString(14), reader.GetInt32(15), reader.IsDBNull(16)?null:reader.GetString(16), reader.IsDBNull(17)?null:reader.GetString(17), reader.GetBoolean(18)));
        await reader.CloseAsync();
        foreach (var h in headers)
        {
            var parcelas = await ListBoletoParcelasAsync(connection, userId, h.Id);
            result.Add(new BoletoDto(h.Id,h.Descricao,h.Beneficiario,h.Categoria,h.SubId,h.SubNome,h.Tipo,h.Valor,h.Frequencia,h.Qtd,h.Inicio,h.Fim,h.ContaId,h.ContaNome,h.Modo,h.Dias,h.Codigo,h.Obs,h.Ativo,parcelas));
        }
        return result;
    }

    private static async Task<IReadOnlyList<BoletoParcelaDto>> ListBoletoParcelasAsync(Npgsql.NpgsqlConnection connection, Guid userId, Guid boletoId)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
          select id,id_boleto,numero_parcela,total_parcelas,valor_previsto,data_vencimento,status,
                 data_pagamento,valor_pago,juros,multa,desconto,id_conta_pagamento,id_saida_gerada,
                 pagamento_automatico,observacao_pagamento
            from public.boleto_parcelas
           where id_usuario=@userId and id_boleto=@boletoId
           order by numero_parcela
        """;
        Add(command,"userId",userId); Add(command,"boletoId",boletoId);
        var rows = new List<BoletoParcelaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while(await reader.ReadAsync()) rows.Add(new BoletoParcelaDto(reader.GetGuid(0),reader.GetGuid(1),reader.GetInt32(2),reader.GetInt32(3),reader.GetDecimal(4),reader.GetFieldValue<DateOnly>(5),reader.GetString(6),reader.IsDBNull(7)?null:reader.GetFieldValue<DateOnly>(7),reader.IsDBNull(8)?null:reader.GetDecimal(8),reader.GetDecimal(9),reader.GetDecimal(10),reader.GetDecimal(11),reader.IsDBNull(12)?null:reader.GetGuid(12),reader.IsDBNull(13)?null:reader.GetGuid(13),reader.GetBoolean(14),reader.IsDBNull(15)?null:reader.GetString(15)));
        return rows;
    }

    public async Task<Guid> InsertBoletoAsync(Guid userId, BoletoRequest request)
    {
        ValidateBoleto(request);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var tx = await connection.BeginTransactionAsync();
        var id = Guid.NewGuid();
        await using (var command = connection.CreateCommand())
        {
            command.Transaction=tx;
            command.CommandText="""insert into public.boletos(id,id_usuario,descricao,beneficiario,categoria,id_subcategoria,tipo,valor_original,frequencia,quantidade_parcelas,data_inicio,data_fim,id_conta_padrao,modo_pagamento,dias_lembrete,codigo_barras,observacao,ativo) values(@id,@userId,@descricao,@beneficiario,@categoria,@sub,@tipo,@valor,@freq,@qtd,@inicio,@fim,@conta,@modo,@dias,@codigo,@obs,@ativo)""";
            Add(command,"id",id); Add(command,"userId",userId); Add(command,"descricao",request.Descricao.Trim()); Add(command,"beneficiario",NullIfWhiteSpace(request.Beneficiario)); Add(command,"categoria",NormalizeCategoria(request.Categoria)); Add(command,"sub",request.IdSubcategoria,NpgsqlDbType.Uuid); Add(command,"tipo",NormalizeBoletoTipo(request.Tipo)); Add(command,"valor",request.ValorOriginal); Add(command,"freq",NormalizeBoletoFrequencia(request.Frequencia)); Add(command,"qtd",ResolveQuantidade(request)); Add(command,"inicio",request.DataInicio,NpgsqlDbType.Date); Add(command,"fim",request.DataFim,NpgsqlDbType.Date); Add(command,"conta",request.IdContaPadrao,NpgsqlDbType.Uuid); Add(command,"modo",NormalizeBoletoModo(request.ModoPagamento)); Add(command,"dias",Math.Clamp(request.DiasLembrete,0,90)); Add(command,"codigo",NullIfWhiteSpace(request.CodigoBarras)); Add(command,"obs",NullIfWhiteSpace(request.Observacao)); Add(command,"ativo",request.Ativo);
            await command.ExecuteNonQueryAsync();
        }
        await RebuildBoletoParcelasAsync(connection,tx,userId,id,request);
        await tx.CommitAsync();
        return id;
    }

    public async Task UpdateBoletoAsync(Guid userId, Guid id, BoletoRequest request)
    {
        ValidateBoleto(request);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var tx = await connection.BeginTransactionAsync();
        await using(var command=connection.CreateCommand())
        {
            command.Transaction=tx;
            command.CommandText="""update public.boletos set descricao=@descricao,beneficiario=@beneficiario,categoria=@categoria,id_subcategoria=@sub,tipo=@tipo,valor_original=@valor,frequencia=@freq,quantidade_parcelas=@qtd,data_inicio=@inicio,data_fim=@fim,id_conta_padrao=@conta,modo_pagamento=@modo,dias_lembrete=@dias,codigo_barras=@codigo,observacao=@obs,ativo=@ativo where id=@id and id_usuario=@userId""";
            Add(command,"id",id); Add(command,"userId",userId); Add(command,"descricao",request.Descricao.Trim()); Add(command,"beneficiario",NullIfWhiteSpace(request.Beneficiario)); Add(command,"categoria",NormalizeCategoria(request.Categoria)); Add(command,"sub",request.IdSubcategoria,NpgsqlDbType.Uuid); Add(command,"tipo",NormalizeBoletoTipo(request.Tipo)); Add(command,"valor",request.ValorOriginal); Add(command,"freq",NormalizeBoletoFrequencia(request.Frequencia)); Add(command,"qtd",ResolveQuantidade(request)); Add(command,"inicio",request.DataInicio,NpgsqlDbType.Date); Add(command,"fim",request.DataFim,NpgsqlDbType.Date); Add(command,"conta",request.IdContaPadrao,NpgsqlDbType.Uuid); Add(command,"modo",NormalizeBoletoModo(request.ModoPagamento)); Add(command,"dias",Math.Clamp(request.DiasLembrete,0,90)); Add(command,"codigo",NullIfWhiteSpace(request.CodigoBarras)); Add(command,"obs",NullIfWhiteSpace(request.Observacao)); Add(command,"ativo",request.Ativo);
            if(await command.ExecuteNonQueryAsync()==0) throw new InvalidOperationException("Boleto não encontrado.");
        }
        await using(var delete=connection.CreateCommand()) { delete.Transaction=tx; delete.CommandText="delete from public.boleto_parcelas where id_boleto=@id and id_usuario=@userId and status='pendente'"; Add(delete,"id",id); Add(delete,"userId",userId); await delete.ExecuteNonQueryAsync(); }
        await RebuildBoletoParcelasAsync(connection,tx,userId,id,request,true);
        await tx.CommitAsync();
    }

    public async Task DeleteBoletoAsync(Guid userId, Guid id)
    {
        await using var connection=await _dataSource.OpenConnectionAsync(); await using var command=connection.CreateCommand(); command.CommandText="delete from public.boletos where id=@id and id_usuario=@userId"; Add(command,"id",id); Add(command,"userId",userId); await command.ExecuteNonQueryAsync();
    }

    public async Task PayBoletoParcelaAsync(Guid userId, Guid parcelaId, BoletoPagamentoRequest request, bool automatico=false)
    {
        if(request.ValorPago<=0) throw new ArgumentException("Informe um valor pago válido.");
        await using var connection=await _dataSource.OpenConnectionAsync(); await using var tx=await connection.BeginTransactionAsync();
        Guid boletoId; string descricao; string? categoria; Guid? sub; Guid? contaPadrao;
        await using(var get=connection.CreateCommand()) { get.Transaction=tx; get.CommandText="""select p.id_boleto,b.descricao,b.categoria,b.id_subcategoria,b.id_conta_padrao from public.boleto_parcelas p join public.boletos b on b.id=p.id_boleto where p.id=@id and p.id_usuario=@userId and p.status='pendente' for update"""; Add(get,"id",parcelaId); Add(get,"userId",userId); await using var r=await get.ExecuteReaderAsync(); if(!await r.ReadAsync()) throw new InvalidOperationException("Parcela pendente não encontrada."); boletoId=r.GetGuid(0); descricao=r.GetString(1); categoria=r.IsDBNull(2)?null:r.GetString(2); sub=r.IsDBNull(3)?null:r.GetGuid(3); contaPadrao=r.IsDBNull(4)?null:r.GetGuid(4); }
        var saidaId=Guid.NewGuid(); var conta=request.IdContaPagamento??contaPadrao;
        await using(var insert=connection.CreateCommand()) { insert.Transaction=tx; insert.CommandText="""insert into public.saidas(id,id_usuario,descricao,categoria,id_subcategoria,valor,data_movimentacao,observacao,id_conta) values(@id,@userId,@descricao,@categoria,@sub,@valor,@data,@obs,@conta)"""; Add(insert,"id",saidaId); Add(insert,"userId",userId); Add(insert,"descricao",descricao); Add(insert,"categoria",categoria); Add(insert,"sub",sub,NpgsqlDbType.Uuid); Add(insert,"valor",request.ValorPago); Add(insert,"data",request.DataPagamento,NpgsqlDbType.Date); Add(insert,"obs",$"Pagamento de boleto. {request.Observacao}".Trim()); Add(insert,"conta",conta,NpgsqlDbType.Uuid); await insert.ExecuteNonQueryAsync(); }
        await using(var update=connection.CreateCommand()) { update.Transaction=tx; update.CommandText="""update public.boleto_parcelas set status='pago',data_pagamento=@data,valor_pago=@valor,juros=@juros,multa=@multa,desconto=@desconto,id_conta_pagamento=@conta,id_saida_gerada=@saida,pagamento_automatico=@automatico,observacao_pagamento=@obs where id=@id and id_usuario=@userId"""; Add(update,"id",parcelaId); Add(update,"userId",userId); Add(update,"data",request.DataPagamento,NpgsqlDbType.Date); Add(update,"valor",request.ValorPago); Add(update,"juros",Math.Max(0,request.Juros)); Add(update,"multa",Math.Max(0,request.Multa)); Add(update,"desconto",Math.Max(0,request.Desconto)); Add(update,"conta",conta,NpgsqlDbType.Uuid); Add(update,"saida",saidaId); Add(update,"automatico",automatico); Add(update,"obs",NullIfWhiteSpace(request.Observacao)); await update.ExecuteNonQueryAsync(); }
        await tx.CommitAsync();
    }

    public async Task<int> ProcessAutomaticBoletosAsync(Guid userId, DateOnly today)
    {
        var boletos=await ListBoletosAsync(userId); var count=0;
        foreach(var b in boletos.Where(x=>x.Ativo && x.ModoPagamento=="automatico" && x.IdContaPadrao.HasValue))
          foreach(var p in b.Parcelas.Where(x=>x.Status=="pendente" && x.DataVencimento<=today)) { await PayBoletoParcelaAsync(userId,p.Id,new BoletoPagamentoRequest(today,p.ValorPrevisto,0,0,0,b.IdContaPadrao,"Pagamento automático no vencimento."),true); count++; }
        return count;
    }

    private static async Task RebuildBoletoParcelasAsync(Npgsql.NpgsqlConnection connection,Npgsql.NpgsqlTransaction tx,Guid userId,Guid boletoId,BoletoRequest request,bool ignoreExisting=false)
    {
        var qtd=ResolveQuantidade(request); var tipo=NormalizeBoletoTipo(request.Tipo); var valorParcela=tipo=="parcelado"?Math.Round(request.ValorOriginal/qtd,2):request.ValorOriginal;
        for(var i=1;i<=qtd;i++)
        {
            var due=AdvanceDate(request.DataInicio,i-1,NormalizeBoletoFrequencia(request.Frequencia)); if(request.DataFim.HasValue && due>request.DataFim.Value) break;
            await using var command=connection.CreateCommand(); command.Transaction=tx; command.CommandText="""insert into public.boleto_parcelas(id_usuario,id_boleto,numero_parcela,total_parcelas,valor_previsto,data_vencimento) values(@userId,@boleto,@numero,@total,@valor,@data) on conflict(id_boleto,numero_parcela) do nothing"""; Add(command,"userId",userId); Add(command,"boleto",boletoId); Add(command,"numero",i); Add(command,"total",qtd); Add(command,"valor",valorParcela); Add(command,"data",due,NpgsqlDbType.Date); await command.ExecuteNonQueryAsync();
        }
    }
    private static DateOnly AdvanceDate(DateOnly date,int step,string freq)=>freq switch {"semanal"=>date.AddDays(step*7),"anual"=>date.AddYears(step),_=>date.AddMonths(step)};
    private static int ResolveQuantidade(BoletoRequest r)=>NormalizeBoletoTipo(r.Tipo) switch {"unico"=>1,"fixo"=>Math.Clamp(r.QuantidadeParcelas<=1?12:r.QuantidadeParcelas,1,120),_=>Math.Clamp(r.QuantidadeParcelas,2,120)};
    private static string NormalizeBoletoTipo(string? v)=>v?.Trim().ToLowerInvariant() switch {"fixo"=>"fixo","parcelado"=>"parcelado",_=>"unico"};
    private static string NormalizeBoletoFrequencia(string? v)=>v?.Trim().ToLowerInvariant() switch {"semanal"=>"semanal","anual"=>"anual",_=>"mensal"};
    private static string NormalizeBoletoModo(string? v)=>v?.Trim().ToLowerInvariant() switch {"automatico"=>"automatico","confirmacao"=>"confirmacao",_=>"manual"};
    private static void ValidateBoleto(BoletoRequest r){if(string.IsNullOrWhiteSpace(r.Descricao))throw new ArgumentException("Informe a descrição."); if(r.ValorOriginal<=0)throw new ArgumentException("Informe um valor maior que zero.");}
}
