namespace JsnFinances.Api.Data;

public sealed class CreditCardsConcurrencyException(long currentVersion) : Exception("Os dados de cartões foram alterados em outra sessão.")
{
    public long CurrentVersion { get; } = currentVersion;
}
