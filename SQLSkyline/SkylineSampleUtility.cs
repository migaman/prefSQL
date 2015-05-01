namespace prefSQL.SQLSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal sealed class SkylineSampleUtility
    {
        private HashSet<HashSet<int>> _subspaces;
        private readonly ISubspacesProducer _subspacesProducer;
        private int _sampleDimension;
        private int _allPreferencesCount;
        private int _sampleCount;

        internal int SampleDimension
        {
            get { return _sampleDimension; }
            set
            {
                _sampleDimension = value;
                SubspacesProducer.SampleDimension = value;
            }
        }

        internal int AllPreferencesCount
        {
            get { return _allPreferencesCount; }
            set
            {
                _allPreferencesCount = value;
                SubspacesProducer.AllPreferencesCount = value;
            }
        }

        internal int SampleCount
        {
            get { return _sampleCount; }
            set
            {
                _sampleCount = value;
                SubspacesProducer.SampleCount = value;
            }
        }

        internal ISubspacesProducer SubspacesProducer
        {
            get { return _subspacesProducer; }
        }

        public SkylineSampleUtility()
            : this(new RandomSubspacesProducer())
        {
        }

        public SkylineSampleUtility(ISubspacesProducer subspacesProducer)
        {
            _subspacesProducer = subspacesProducer;
        }

        internal HashSet<HashSet<int>> Subspaces
        {
            get { return _subspaces ?? (_subspaces = DetermineSubspaces()); }
            private set { _subspaces = value; }
        }


        private HashSet<HashSet<int>> DetermineSubspaces()
        {
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

            var subspacesReturn = SubspacesProducer.GetSubspaces();

            if (subspacesReturn.Count != SampleCount)
            {
                throw new Exception("Not produced the correct number of subspaces.");
            }

            if (subspacesReturn.Any(subspace => subspace.Count != SampleDimension))
            {
                throw new Exception("Produced subspace of incorrect dimension.");
            }

            if (!AreAllPreferencesAtLeastOnceContainedInSubspaces(subspacesReturn))
            {
                throw new Exception("Not all preferences at least once contained in produced subspaces.");
            }

            foreach (var subspaceReturn in subspacesReturn)
            {
                if (subspacesReturn.Where(element => element != subspaceReturn).Any(element => element.SetEquals(subspaceReturn)))
                {
                    throw new Exception("Same subspace contained multiple times.");
                }
            }

            return subspacesReturn;
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
    }
}