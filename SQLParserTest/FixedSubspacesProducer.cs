namespace prefSQL.SQLParserTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using prefSQL.SQLSkyline;

    public sealed class FixedSubspacesProducer : ISubspacesProducer
    {
        private readonly HashSet<HashSet<int>> _fixedSubspaces;

        public int SampleDimension { get; set; }

        public int SampleCount { get; set; }

        public int AllPreferencesCount { get; set; }

        public FixedSubspacesProducer(HashSet<HashSet<int>> fixedSubspaces)
        {
            _fixedSubspaces = fixedSubspaces;
        }

        private HashSet<HashSet<int>> FixedSubspaces
        {
            get { return _fixedSubspaces; }
        }

        public HashSet<HashSet<int>> GetSubspaces()
        {
            var subspacesReturn = new HashSet<HashSet<int>>();
            foreach (var subspace in FixedSubspaces.Select(fixedSubspace => new HashSet<int>(fixedSubspace)))
            {
                subspacesReturn.Add(subspace);
            }
            return subspacesReturn;
        }
    }
}