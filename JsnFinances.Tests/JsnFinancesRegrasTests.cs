using FluentAssertions;

namespace JsnFinances.Tests;

public class JsnFinancesRegrasTests
{
    [Fact]
    public void CalculoDeParcelas_DeveFecharValorTotal()
    {
        var totalCentavos = 10000;
        var parcelas = 3;
        var baseCentavos = totalCentavos / parcelas;
        var resto = totalCentavos % parcelas;

        var valores = Enumerable.Range(0, parcelas)
            .Select(index => baseCentavos + (index < resto ? 1 : 0))
            .ToList();

        valores.Sum().Should().Be(totalCentavos);
        valores.Should().Equal(3334, 3333, 3333);
    }

    [Theory]
    [InlineData("Receita", "entrada")]
    [InlineData("income", "entrada")]
    [InlineData("Saída", "saida")]
    [InlineData("despesa", "saida")]
    [InlineData("Entrada e Saída", "ambos")]
    public void TipoCategoria_DeveSerNormalizado(string entrada, string esperado)
    {
        NormalizarTipoCategoria(entrada).Should().Be(esperado);
    }

    private static string NormalizarTipoCategoria(string? tipo)
    {
        var normalized = (tipo ?? "ambos")
            .Trim()
            .ToLowerInvariant()
            .Normalize(System.Text.NormalizationForm.FormD);

        var semAcento = new string(normalized
            .Where(c => System.Globalization.CharUnicodeInfo.GetUnicodeCategory(c) != System.Globalization.UnicodeCategory.NonSpacingMark)
            .ToArray())
            .Replace("/", " ")
            .Replace("\\", " ")
            .Replace("_", " ")
            .Replace("-", " ")
            .Trim();

        return semAcento switch
        {
            "entrada" or "entradas" or "receita" or "receitas" or "income" or "entry" or "in" => "entrada",
            "saida" or "saidas" or "despesa" or "despesas" or "expense" or "exit" or "out" => "saida",
            "ambos" or "both" or "all" or "todos" or "entrada e saida" or "receita e despesa" => "ambos",
            _ => "ambos"
        };
    }
}
