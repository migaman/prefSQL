using System.Collections.Generic;
using System.Linq;

namespace prefSQL.SQLParserTest
{
    using SQLSkyline.SkylineSampling;

    public sealed class FixedSkylineSamplingSubspacesProducer : ISkylineSamplingSubspacesProducer
    {
        private readonly HashSet<HashSet<int>> _fixedSubspaces;

        public int SubspacesCount { get; set; }

        public int SubspaceDimension { get; set; }

        public int AllPreferencesCount { get; set; }

        public FixedSkylineSamplingSubspacesProducer(HashSet<HashSet<int>> fixedSubspaces)
        {
            _fixedSubspaces = fixedSubspaces;
        }

        private IEnumerable<HashSet<int>> FixedSubspaces
        {
            get { return _fixedSubspaces; }
        }

        public HashSet<HashSet<int>> GetSubspaces()
        {
            var subspacesReturn = new HashSet<HashSet<int>>();
            foreach (HashSet<int> subspace in FixedSubspaces.Select(fixedSubspace => new HashSet<int>(fixedSubspace)))
            {
                subspacesReturn.Add(subspace);
            }
            return subspacesReturn;
        }
    }
}