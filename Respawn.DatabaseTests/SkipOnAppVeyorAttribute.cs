namespace Respawn.DatabaseTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Xunit;
    using Xunit.Abstractions;
    using Xunit.Sdk;

    public class SkipOnAppVeyorTestDiscoverer : IXunitTestCaseDiscoverer
    {
        private readonly IMessageSink _diagnosticMessageSink;

        public SkipOnAppVeyorTestDiscoverer(IMessageSink diagnosticMessageSink)
        {
            _diagnosticMessageSink = diagnosticMessageSink;
        }

        public IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod, IAttributeInfo factAttribute)
        {
            if (Environment.GetEnvironmentVariable("Appveyor")?.ToUpperInvariant() == "TRUE")
            {
                return Enumerable.Empty<IXunitTestCase>();
            }

            return new[] { new XunitTestCase(_diagnosticMessageSink, discoveryOptions.MethodDisplayOrDefault(), testMethod) };
        }
    }

    [XunitTestCaseDiscoverer("Respawn.DatabaseTests.SkipOnAppVeyorTestDiscoverer", "Respawn.DatabaseTests")]
    public class SkipOnAppVeyorAttribute : FactAttribute
    {
    }
}
