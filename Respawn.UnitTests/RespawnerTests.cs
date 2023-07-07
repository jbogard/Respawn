using System;
using System.Threading.Tasks;
using Shouldly;
using Xunit;

namespace Respawn.UnitTests;

public class RespawnerTests
{
    [Fact]
    public async Task Should_throw_when_adapter_not_SQL()
    {
        var exception = await Should.ThrowAsync<ArgumentException>(Respawner.CreateAsync("Server=(LocalDb)\\mssqllocaldb;Database=SqlServerTests;Integrated Security=True", new RespawnerOptions
        {
            DbAdapter = DbAdapter.MySql
        }));
    }
}