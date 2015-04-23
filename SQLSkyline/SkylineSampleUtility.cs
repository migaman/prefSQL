namespace prefSQL.SQLSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal sealed class SkylineSampleUtility
    {
        private static readonly Random MyRandom = new Random();
        private HashSet<HashSet<int>> _subspaces;
        private readonly int _sampleDimension;
        private readonly int _sampleCount;
        private readonly List<int> _allPreferences;

        public int SampleDimension
        {
            get { return _sampleDimension; }
        }

        public List<int> AllPreferences
        {
            get { return _allPreferences; }
        }

        public int SampleCount
        {
            get { return _sampleCount; }
        }

        public SkylineSampleUtility(List<int> allPreferences, int sampleCount, int sampleDimension)
        {
            _allPreferences = allPreferences;
            _sampleCount = sampleCount;
            _sampleDimension = sampleDimension;
        }

        internal HashSet<HashSet<int>> Subspaces
        {
            get
            {
                if (_subspaces == null)
                {
                    DetermineSubspaces();
                }
                return _subspaces;
            }
            private set { _subspaces = value; }
        }

        private void DetermineSubspaces()
        {
            var subspacesReturn = new HashSet<HashSet<int>>();

            var done = false;
            while (!done)
            {
                if (subspacesReturn.Count >= SampleCount)
                {
                    if (AreAllPreferencesAtLeastOnceContainedInSubspaces(subspacesReturn))
                    {
                        done = true;
                    }
                    else
                    {
                        RemoveOneSubspace(subspacesReturn);
                    }
                }
                else
                {
                    AddOneSubspace(subspacesReturn);
                }
            }

            Subspaces = subspacesReturn;
        }

        private bool AreAllPreferencesAtLeastOnceContainedInSubspaces(
            IEnumerable<HashSet<int>> subspaceQueries)
        {
            var allPreferences = new List<int>(AllPreferences);

            foreach (var subspaceQueryPreferences in subspaceQueries)
            {
                allPreferences.RemoveAll(subspaceQueryPreferences.Contains);
            }

            return allPreferences.Count == 0;
        }

        private static void RemoveOneSubspace(ICollection<HashSet<int>> subspaceQueries)
        {
            subspaceQueries.Remove(subspaceQueries.ElementAt(MyRandom.Next(subspaceQueries.Count)));
        }

        private void AddOneSubspace(ISet<HashSet<int>> subspaceQueries)
        {
            HashSet<int> subspaceQueryCandidate;

            do
            {
                subspaceQueryCandidate = new HashSet<int>();

                while (subspaceQueryCandidate.Count < SampleDimension)
                {
                    subspaceQueryCandidate.Add(AllPreferences[MyRandom.Next(AllPreferences.Count)]);
                }
            } while (IsSubspaceQueryCandidateContainedWithinSubspaceQueries(subspaceQueryCandidate, subspaceQueries));

            subspaceQueries.Add(subspaceQueryCandidate);
        }

        private static bool IsSubspaceQueryCandidateContainedWithinSubspaceQueries(
            IEnumerable<int> subspaceQueryCandidate, IEnumerable<HashSet<int>> subspaceQueries)
        {
            return subspaceQueries.Any(element => element.SetEquals(subspaceQueryCandidate));
        }

        public HashSet<int> GetSubspaceComplement(HashSet<int> subspace)
        {
            var allPreferences = new List<int>(AllPreferences);
            allPreferences.RemoveAll(subspace.Contains);
            return new HashSet<int>(allPreferences);
        }
    }
}