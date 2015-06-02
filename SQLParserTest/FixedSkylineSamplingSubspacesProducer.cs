using System.Collections.Generic;
using System.Linq;

namespace prefSQL.SQLParserTest
{
    using SQLSkyline;
    using SQLSkyline.SkylineSampling;

    public sealed class FixedSkylineSamplingSubspacesProducer : ISkylineSamplingSubspacesProducer
    {
        private readonly IEnumerable<CLRSafeHashSet<int>> _fixedSubspaces;

        public int SubspacesCount { get; set; }

        public int SubspaceDimension { get; set; }

        public int AllPreferencesCount { get; set; }

        public FixedSkylineSamplingSubspacesProducer(IEnumerable<CLRSafeHashSet<int>> fixedSubspaces)
        {
            _fixedSubspaces = fixedSubspaces;
        }

        private IEnumerable<CLRSafeHashSet<int>> FixedSubspaces
        {
            get { return _fixedSubspaces; }
        }

        public IEnumerable<CLRSafeHashSet<int>> GetSubspaces()
        {
            return FixedSubspaces.Select(fixedSubspace => new CLRSafeHashSet<int>(fixedSubspace));
        }
    }
}