namespace prefSQL.SQLSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal sealed class RandomSubspacesesProducer : ISubspacesProducer
    {
        private static readonly Random MyRandom = new Random();

        public int SampleDimension { get; set; }

        public int SampleCount { get; set; }

        public int AllPreferencesCount { get; set; }
     
        public HashSet<HashSet<int>> GetSubspaces()
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