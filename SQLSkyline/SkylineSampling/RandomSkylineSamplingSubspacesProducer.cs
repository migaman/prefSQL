namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    ///     Produces randomly chosen subsets out of the preferences requested in original skyline query.
    /// </summary>
    /// <remarks>
    ///     This strategy represents the originally proposed method of choosing the subspaces and should be used whenever this
    ///     original semantics is desired.
    /// </remarks>
    internal sealed class RandomSkylineSamplingSubspacesProducer : ISkylineSamplingSubspacesProducer
    {
        private static readonly Random MyRandom = new Random();

        public int SubspacesCount { get; set; }

        public int SubspaceDimension { get; set; }

        public int AllPreferencesCount { get; set; }

        /// <summary>
        ///     Get all subspaces that the skyline sampling algorithm will use to calculate its subspace skylines. The subspaces
        ///     are calculated randomly.
        /// </summary>
        /// <remarks>
        ///     As long as the desired number of subspaces (i.e., SubspacesCount) is not reached, new subspaces are randomly
        ///     produced. When the number is reached, it is checked whether all preferences requested in the original skyline query
        ///     are contained in at least on subspace. If this is not the case, one randomly chosen produced subspace is removed
        ///     and the process continues to produce and remove subspaces until both conditions are met.
        /// </remarks>
        /// <returns>
        ///     The produced subspaces. Each stored integer is an index referring the zero-based position of a preference of
        ///     the original skyline query.
        /// </returns>
        /// <exception cref="Exception">Thrown on invocation of SkylineSamplingUtility.CheckValidityOfCountAndDimension.</exception>
        public HashSet<HashSet<int>> GetSubspaces()
        {
            SkylineSamplingUtility.CheckValidityOfCountAndDimension(SubspacesCount, SubspaceDimension, AllPreferencesCount);

            var subspacesReturn = new HashSet<HashSet<int>>();

            while (!IsSubspaceProductionComplete(subspacesReturn))
            {
                if (subspacesReturn.Count >= SubspacesCount)
                {
                    RemoveOneSubspaceRandomly(subspacesReturn);
                }

                AddOneRandomSubspaceNotYetContained(subspacesReturn);
            }

            return subspacesReturn;
        }

        private bool IsSubspaceProductionComplete(ICollection<HashSet<int>> subspacesReturn)
        {
            return subspacesReturn.Count == SubspacesCount &&
                   AreAllPreferencesAtLeastContainedOnceInSubspaces(subspacesReturn);
        }

        private bool AreAllPreferencesAtLeastContainedOnceInSubspaces(
            IEnumerable<HashSet<int>> subspaceQueries)
        {
            var containedPreferences = new HashSet<int>();

            foreach (HashSet<int> subspaceQueryPreferences in subspaceQueries)
            {
                foreach (int subspaceQueryPreference in subspaceQueryPreferences)
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

        private static void RemoveOneSubspaceRandomly(ICollection<HashSet<int>> subspaceQueries)
        {
            subspaceQueries.Remove(subspaceQueries.ElementAt(MyRandom.Next(subspaceQueries.Count)));
        }

        private void AddOneRandomSubspaceNotYetContained(ISet<HashSet<int>> subspaceQueries)
        {
            HashSet<int> subspaceQueryCandidate;

            do
            {
                subspaceQueryCandidate = new HashSet<int>();

                while (subspaceQueryCandidate.Count < SubspaceDimension)
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