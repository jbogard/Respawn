using System.Globalization;

namespace Respawn
{
    public class ParameterNameGenerator
    {
        private int _count;

        public virtual string GenerateNext() => string.Format(CultureInfo.InvariantCulture, "@p{0}", _count++);

        public virtual void Reset() => _count = 0;
    }
}
