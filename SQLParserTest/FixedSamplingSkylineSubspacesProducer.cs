namespace prefSQL.SQLParserTest
{
    using System.Collections.Generic;
    using System.Linq;
    using prefSQL.SQLSkyline;

    public sealed class FixedSamplingSkylineSubspacesProducer : ISamplingSkylineSubspacesProducer
    {
        private readonly HashSet<HashSet<int>> _fixedSubspaces;

        public int SubspacesCount { get; set; }

        public int SubspaceDimension { get; set; }

        public int AllPreferencesCount { get; set; }

        public FixedSamplingSkylineSubspacesProducer(HashSet<HashSet<int>> fixedSubspaces)
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