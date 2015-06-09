namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal sealed class SkylineSamplingUtility
    {
        private readonly ISkylineSamplingSubsetsProducer _subsetsProducer;
        private int _allPreferencesCount;
        private int _subsetCount;
        private int _subsetDimension;
        private IEnumerable<CLRSafeHashSet<int>> _subsets;

        /// <summary>
        ///     All Operators over the preferences (e.g., "LOW", "INCOMPARABLE").
        /// </summary>
        internal string[] Operators { get; set; }

        /// <summary>
        ///     All strings for the Operators (e.g., "LOW", "LOW;INCOMPARABLE").
        /// </summary>
        /// <remarks>
        ///     Since INCOMPARABLE preferences are represented by two columns resp. operators, OperatorStrings is used to represent
        ///     this situation. There are two possibilities: Either an element of OperatorStrings contains "LOW", or it contains
        ///     "LOW;INCOMPARABLE". This is also used for convenience when executing the skyline algorithm over a subset of all
        ///     preferences - the operators for this subset can be simply concatenated from the OperatorStrings property.
        /// </remarks>
        internal string[] OperatorStrings { get; set; }

        /// <summary>
        ///     The positions of the preferences (i.e., the columns) within the skyline reported from the skyline algorithm.
        /// </summary>
        /// <remarks>
        ///     This is used to skip over INCOMPARABLE columns when they're not used (e.g., when collecting the skylineValues for
        ///     sorting methods).
        /// </remarks>
        internal int[] PreferenceColumnIndex { get; set; }

        internal int SubsetCount
        {
            get { return _subsetCount; }
            set
            {
                _subsetCount = value;
                SubsetsProducer.SubsetsCount = value;
            }
        }

        internal int SubsetDimension
        {
            get { return _subsetDimension; }
            set
            {
                _subsetDimension = value;
                SubsetsProducer.SubsetDimension = value;
            }
        }

        internal int AllPreferencesCount
        {
            get { return _allPreferencesCount; }
            set
            {
                _allPreferencesCount = value;
                SubsetsProducer.AllPreferencesCount = value;
            }
        }

        internal int ArtificialUniqueRowIdentifierColumnIndex { get; set; }
        internal int EqualValuesBucketColumnIndex { get; set; }

        internal ISkylineSamplingSubsetsProducer SubsetsProducer
        {
            get { return _subsetsProducer; }
        }

        internal IEnumerable<CLRSafeHashSet<int>> Subsets
        {
            get { return _subsets ?? (_subsets = DetermineSubsets()); }
        }

        public bool[] IsPreferenceIncomparable { get; set; }

        public SkylineSamplingUtility()
            : this(new RandomSkylineSamplingSubsetsProducer())
        {
        }

        public SkylineSamplingUtility(ISkylineSamplingSubsetsProducer subsetsProducer)
        {
            _subsetsProducer = subsetsProducer;
        }

        private IEnumerable<CLRSafeHashSet<int>> DetermineSubsets()
        {
            CheckValidityOfCountAndDimension(SubsetCount, SubsetDimension, AllPreferencesCount);

            IList<CLRSafeHashSet<int>> subsetsReturn = SubsetsProducer.GetSubsets().ToList();

            if (subsetsReturn.Count != SubsetCount)
            {
                throw new Exception("Not produced the correct number of subsets.");
            }

            if (subsetsReturn.Any(subset => subset.Count != SubsetDimension))
            {
                throw new Exception("Produced subset of incorrect dimension.");
            }

            if (!AreAllPreferencesAtLeastOnceContainedInSubset(subsetsReturn))
            {
                throw new Exception("Not all preferences at least once contained in produced subsets.");
            }

            foreach (CLRSafeHashSet<int> subsetReturn in subsetsReturn)
            {
                CLRSafeHashSet<int> subsetReturnLocal = subsetReturn;

                if (
                    subsetsReturn.Where(element => element != subsetReturnLocal)
                        .Any(element => element.SetEquals(subsetReturnLocal)))
                {
                    throw new Exception("Same subset contained multiple times.");
                }
            }

            return subsetsReturn;
        }

        /// <summary>
        ///     Checks whether the provided parameters are valid for the production of the requested subsets of preferences.
        /// </summary>
        /// <remarks>
        ///     The provided parameters are valid if all preferences requested in the original skyline can be included in
        ///     at least one subset (i.e., subsetCount * subsetDimension is larger than or equal to allPreferencesCount) and
        ///     if there are not more distinct subsets requested than can possibly be produced (i.e., subsetsCount is lower
        ///     than or equal to the binomial coefficient ("n choose k", i.e. "allPreferencesCount choose subsetDimension")).
        /// </remarks>
        /// <param name="subsetsCount">Number of desired subsets.</param>
        /// <param name="subsetDimension">Dimensionality of each subset.</param>
        /// <param name="allPreferencesCount">Number of all preferences requested in original skyline query.</param>
        /// <exception cref="Exception">
        ///     Thrown when the provided parameters are not valid according to the conditions specified in the remarks.
        /// </exception>
        internal static void CheckValidityOfCountAndDimension(int subsetsCount, int subsetDimension,
            int allPreferencesCount)
        {
            if (subsetsCount * subsetDimension < allPreferencesCount)
            {
                throw new Exception(
                    string.Format(
                        "Every preference has to be included in at least one subset. This is not possible, since there are {0} preferences and at most COUNT (= {1}) * DIMENSION (= {2}) = {3} of them are included",
                        allPreferencesCount, subsetsCount, subsetDimension, subsetsCount * subsetDimension));
            }

            int binomialCoefficient = BinomialCoefficient(allPreferencesCount, subsetDimension);

            if (subsetsCount > binomialCoefficient)
            {
                throw new Exception(
                    string.Format(
                        "Cannot choose {0} from {1} in order to gain {2} subsets, at most {3} subsets possible.",
                        subsetDimension, allPreferencesCount, subsetsCount, binomialCoefficient));
            }
        }

        private bool AreAllPreferencesAtLeastOnceContainedInSubset(
            IEnumerable<CLRSafeHashSet<int>> subsetQueries)
        {
            var containedPreferences = new CLRSafeHashSet<int>();

            foreach (CLRSafeHashSet<int> subsetQueryPreferences in subsetQueries)
            {
                foreach (int subsetQueryPreference in subsetQueryPreferences)
                {
                    containedPreferences.Add(subsetQueryPreference);
                }
                if (containedPreferences.Count == AllPreferencesCount)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        ///     Calculate the binomial coefficient (n choose k).
        /// </summary>
        /// <remarks>
        ///     Implemented via a multiplicative formula, see
        ///     http://en.wikipedia.org/wiki/Binomial_coefficient#Multiplicative_formula
        /// </remarks>
        /// <param name="n">choose from set n</param>
        /// <param name="k">choose k elements from set n</param>
        /// <returns>Binomial coefficient from n choose k. Returns 0 if n &lt;= 0 or k &gt; n or k &lt; 0.</returns>
        internal static int BinomialCoefficient(int n, int k)
        {
            if (n <= 0)
            {
                return 0;
            }

            if (k > n || k < 0)
            {
                return 0;
            }

            var binomialCoefficient = 1;
            for (var i = 1; i <= k; i++)
            {
                binomialCoefficient *= n + 1 - i;
                binomialCoefficient /= i;
            }

            return binomialCoefficient;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="subset"></param>
        /// <returns></returns>
        public CLRSafeHashSet<int> GetSubsetComplement(CLRSafeHashSet<int> subset)
        {
            var subsetComplement = new CLRSafeHashSet<int>();
            for (var i = 0; i < AllPreferencesCount; i++)
            {
                if (!subset.Contains(i))
                {
                    subsetComplement.Add(i);
                }
            }
            return subsetComplement;
        }

        /// <summary>
        ///     Fill Operators, OperatorStrings and PreferenceColumnIndex properties.
        /// </summary>
        /// <param name="operators">
        ///     The operators with which the preferences are handled; can be either "LOW" or "INCOMPARABLE",
        ///     specified in the format "LOW;LOW;INCOMPARABLE;LOW;LOW;...", i.e., separated via ";".
        /// </param>
        internal void CalculatePropertiesWithRespectToIncomparableOperators(string operators)
        {
            Operators = operators.Split(';');
            int prefrencesCount = Operators.Count(op => op != "INCOMPARABLE");
            OperatorStrings = new string[prefrencesCount];
            PreferenceColumnIndex = new int[prefrencesCount];
            IsPreferenceIncomparable = new bool[prefrencesCount];

            var nextOperatorIndex = 0;
            for (var opIndex = 0; opIndex < Operators.Length; opIndex++)
            {
                if (Operators[opIndex] != "INCOMPARABLE")
                {
                    IsPreferenceIncomparable[nextOperatorIndex] = false;
                    OperatorStrings[nextOperatorIndex] = Operators[opIndex];
                    PreferenceColumnIndex[nextOperatorIndex] = opIndex;
                    nextOperatorIndex++;
                }
                else
                {
                    IsPreferenceIncomparable[nextOperatorIndex - 1] = true;
                    // keep "LOW;INCOMPARABLE" together
                    OperatorStrings[nextOperatorIndex - 1] += ";" + Operators[opIndex];
                }
            }
        }
    }
}