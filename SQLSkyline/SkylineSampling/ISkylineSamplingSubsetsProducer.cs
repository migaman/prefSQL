namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System.Collections.Generic;

    /// <summary>
    ///     Interface to calculate and provide the necessary subsets to the skyline sampling algorithm.
    /// </summary>
    /// <remarks>
    ///     Since the skyline sampling algorithm calculates a number of low-dimensional subset skylines (lower than the
    ///     number of specified preferences for the entire skyline), there has to be a means to determine these subsets.
    ///     Originally, they are produced randomly, but implementing this interface allows for different approaches (however,
    ///     of course not without altering the original semantics of the skyline sampling algorithm, which is based on randomly
    ///     choosing these subsets).
    /// </remarks>
    internal interface ISkylineSamplingSubsetsProducer
    {
        /// <summary>
        ///     Number of desired subsets.
        /// </summary>
        int SubsetsCount { get; set; }

        /// <summary>
        ///     Dimensionality of each subset.
        /// </summary>
        int SubsetDimension { get; set; }

        /// <summary>
        ///     Number of all preferences requested in the entire skyline query.
        /// </summary>
        int AllPreferencesCount { get; set; }

        /// <summary>
        ///     Get all subsets that the skyline sampling algorithm will use to calculate its subset skylines.
        /// </summary>
        /// <returns>
        ///     The produced subsets. Each stored integer is an index referring the zero-based position of a preference of
        ///     the entire skyline query.
        /// </returns>
        IEnumerable<CLRSafeHashSet<int>> GetSubsets();
    }
}