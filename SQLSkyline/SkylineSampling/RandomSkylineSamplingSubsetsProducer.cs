namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    ///     Produces randomly chosen subsets out of the preferences requested in original skyline query.
    /// </summary>
    /// <remarks>
    ///     This strategy represents the originally proposed method of choosing the subsets and should be used whenever this
    ///     original semantics is desired.
    /// </remarks>
    internal sealed class RandomSkylineSamplingSubsetsProducer : ISkylineSamplingSubsetsProducer
    {
        private static readonly Random MyRandom = new Random();
        public int SubsetsCount { get; set; }
        public int SubsetDimension { get; set; }
        public int AllPreferencesCount { get; set; }

        /// <summary>
        ///     Get all subsets that the skyline sampling algorithm will use to calculate its subset skylines. The subsets
        ///     are calculated randomly.
        /// </summary>
        /// <remarks>
        ///     As long as the desired number of subsets (i.e., SubsetsCount) is not reached, new subsets are randomly
        ///     produced. When the number is reached, it is checked whether all preferences requested in the original skyline query
        ///     are contained in at least on subset. If this is not the case, one randomly chosen produced subset is removed
        ///     and the process continues to produce and remove subsets until both conditions are met.
        /// </remarks>
        /// <returns>
        ///     The produced subsets. Each stored integer is an index referring the zero-based position of a preference of
        ///     the original skyline query.
        /// </returns>
        /// <exception cref="Exception">Thrown on invocation of SkylineSamplingUtility.CheckValidityOfCountAndDimension.</exception>
        public IEnumerable<CLRSafeHashSet<int>> GetSubsets()
        {
            SkylineSamplingUtility.CheckValidityOfCountAndDimension(SubsetsCount, SubsetDimension, AllPreferencesCount);

            var subsetsReturn = new List<CLRSafeHashSet<int>>();

            while (!IsSubsetProductionComplete(subsetsReturn))
            {
                if (subsetsReturn.Count >= SubsetsCount)
                {
                    RemoveOneSubsetRandomly(subsetsReturn);
                }

                AddOneRandomSubsetNotYetContained(subsetsReturn);
            }

            return subsetsReturn;
        }

        private bool IsSubsetProductionComplete(IReadOnlyCollection<CLRSafeHashSet<int>> subsetsReturn)
        {
            return subsetsReturn.Count == SubsetsCount &&
                   AreAllPreferencesAtLeastContainedOnceInSubsets(subsetsReturn);
        }

        private bool AreAllPreferencesAtLeastContainedOnceInSubsets(
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

        private static void RemoveOneSubsetRandomly(ICollection<CLRSafeHashSet<int>> subsetQueries)
        {
            subsetQueries.Remove(subsetQueries.ElementAt(MyRandom.Next(subsetQueries.Count)));
        }

        private void AddOneRandomSubsetNotYetContained(ICollection<CLRSafeHashSet<int>> subsetQueries)
        {
            CLRSafeHashSet<int> subsetQueryCandidate;

            do
            {
                subsetQueryCandidate = new CLRSafeHashSet<int>();

                while (subsetQueryCandidate.Count < SubsetDimension)
                {
                    subsetQueryCandidate.Add(MyRandom.Next(AllPreferencesCount));
                }
            } while (IsSubsetQueryCandidateContainedWithinSubsetQueries(subsetQueryCandidate, subsetQueries));

            subsetQueries.Add(subsetQueryCandidate);
        }

        private static bool IsSubsetQueryCandidateContainedWithinSubsetQueries(
            IEnumerable<int> subsetQueryCandidate, IEnumerable<CLRSafeHashSet<int>> subsetQueries)
        {
            return subsetQueries.Any(element => element.SetEquals(subsetQueryCandidate));
        }
    }
}