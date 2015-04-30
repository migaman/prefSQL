namespace prefSQL.SQLSkyline
{
    using System.Collections.Generic;

    internal interface ISubspacesProducer
    {
        int SampleDimension { get; set; }
        int SampleCount { get; set; }
        int AllPreferencesCount { get; set; }

        HashSet<HashSet<int>> GetSubspaces();
    }
}