namespace Respawn.DatabaseTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Xunit;
    using Xunit.Abstractions;
    using Xunit.Sdk;

    public class SkipOnCITestDiscoverer : IXunitTestCaseDiscoverer
    {
        private readonly IMessageSink _diagnosticMessageSink;

        public SkipOnCITestDiscoverer(IMessageSink diagnosticMessageSink)
        {
            _diagnosticMessageSink = diagnosticMessageSink;
        }

        public IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod, IAttributeInfo factAttribute)
        {
            if (Environment.GetEnvironmentVariable("CI")?.ToUpperInvariant() == "TRUE")
            {
                return Enumerable.Empty<IXunitTestCase>();
            }

            return new[] { new XunitTestCase(_diagnosticMessageSink, discoveryOptions.MethodDisplayOrDefault(), TestMethodDisplayOptions.All, testMethod) };
        }
    }

    [XunitTestCaseDiscoverer("Respawn.DatabaseTests.SkipOnCITestDiscoverer", "Respawn.DatabaseTests")]
    public class SkipOnCIAttribute : FactAttribute
    {
    }
}
