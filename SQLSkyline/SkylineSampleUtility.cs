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
        private readonly int _allPreferencesCount;

        public int SampleDimension
        {
            get { return _sampleDimension; }
        }

        public int AllPreferencesCount
        {
            get { return _allPreferencesCount; }
        }

        public int SampleCount
        {
            get { return _sampleCount; }
        }

        public SkylineSampleUtility(int allPreferencesCount, int sampleCount, int sampleDimension)
        {
            _allPreferencesCount = allPreferencesCount;
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
            Subspaces = null;

            if (SampleCount*SampleDimension < AllPreferencesCount)
            {
                throw new Exception(
                    string.Format(
                        "Every preference has to be included in at least one subspace. This is not possible, since there are {0} preferences and at most COUNT (= {1}) * DIMENSION (= {2}) = {3} of them are included",
                        AllPreferencesCount, SampleCount, SampleDimension, SampleCount*SampleDimension));
            }

            var binomialCoefficient = QuickBinomialCoefficient(AllPreferencesCount, SampleDimension);

            if (SampleCount > binomialCoefficient)
            {
                throw new Exception(
                    string.Format(
                        "Cannot choose {0} from {1} in order to gain {2} subspaces, at most {3} subspaces possible.",
                        SampleDimension, AllPreferencesCount, SampleCount, binomialCoefficient));
            }

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

        /// <summary>
        ///     calculate binomial coefficient (n choose k).
        /// </summary>
        /// <remarks>
        ///     implemented via a multiplicative formula, see
        ///     http://en.wikipedia.org/wiki/Binomial_coefficient#Multiplicative_formula
        /// </remarks>
        /// <param name="nUpper">choose from set n</param>
        /// <param name="kLower">choose k elements from set n</param>
        /// <returns>binomial coefficient from n choose k</returns>
        private static decimal QuickBinomialCoefficient(int nUpper, int kLower)
        {
            var binomialCoefficient = 1;
            for (var i = 1; i <= kLower; i++)
            {
                binomialCoefficient *= nUpper + 1 - i;
                binomialCoefficient /= i;
            }
            return binomialCoefficient;
        }

        private bool AreAllPreferencesAtLeastOnceContainedInSubspaces(
            IEnumerable<HashSet<int>> subspaceQueries)
        {
            var containedPreferences = new HashSet<int>();

            foreach (var subspaceQueryPreferences in subspaceQueries)
            {
                foreach (var subspaceQueryPreference in subspaceQueryPreferences)
                {
                    containedPreferences.Add(subspaceQueryPreference);
                }
                if (containedPreferences.Count == AllPreferencesCount)
                {
                    return true;
                }
            }

            return false;
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
                    subspaceQueryCandidate.Add(MyRandom.Next(AllPreferencesCount));
                }
            } while (IsSubspaceQueryCandidateContainedWithinSubspaceQueries(subspaceQueryCandidate, subspaceQueries));

            subspaceQueries.Add(subspaceQueryCandidate);
        }

        private static bool IsSubspaceQueryCandidateContainedWithinSubspaceQueries(
            IEnumerable<int> subspaceQueryCandidate, IEnumerable<HashSet<int>> subspaceQueries)
        {
            return subspaceQueries.Any(element => element.SetEquals(subspaceQueryCandidate));
        }
    }
}