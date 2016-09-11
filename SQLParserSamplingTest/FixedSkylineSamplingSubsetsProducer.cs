using System.Collections.Generic;
using System.Linq;

namespace prefSQL.SQLParserSamplingTest
{
    using SQLSkyline;
    using SQLSkyline.SkylineSampling;

    public sealed class FixedSkylineSamplingSubsetsProducer : ISkylineSamplingSubsetsProducer
    {
        private readonly IEnumerable<CLRSafeHashSet<int>> _fixedSubsets;

        public int SubsetsCount { get; set; }

        public int SubsetDimension { get; set; }

        public int AllPreferencesCount { get; set; }

        public FixedSkylineSamplingSubsetsProducer(IEnumerable<CLRSafeHashSet<int>> fixedSubsets)
        {
            _fixedSubsets = fixedSubsets;
        }

        private IEnumerable<CLRSafeHashSet<int>> FixedSubsets
        {
            get { return _fixedSubsets; }
        }

        public IEnumerable<CLRSafeHashSet<int>> GetSubsets()
        {
            return FixedSubsets.Select(fixedSubset => new CLRSafeHashSet<int>(fixedSubset));
        }
    }
}