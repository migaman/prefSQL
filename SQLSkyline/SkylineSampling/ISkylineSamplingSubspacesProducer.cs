namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System.Collections.Generic;

    /// <summary>
    ///     Interface to calculate and provide the necessary subspaces to the skyline sampling algorithm.
    /// </summary>
    /// <remarks>
    ///     Since the skyline sampling algorithm calculates a number of lower-dimensional subspace skylines (lower than the
    ///     number of specified preferences for the entire skyline), there has to be a means to determine these subspaces.
    ///     Originally, they are produced randomly, but implementing this interface allows for different approaches (however,
    ///     of course not without altering the original semantics of the skyline sampling algorithm, which is based on randomly
    ///     choosing these subspaces).
    /// </remarks>
    internal interface ISkylineSamplingSubspacesProducer
    {
        /// <summary>
        ///     Number of desired subspaces.
        /// </summary>
        int SubspacesCount { get; set; }

        /// <summary>
        ///     Dimensionality of each subspace.
        /// </summary>
        int SubspaceDimension { get; set; }

        /// <summary>
        ///     Number of all preferences requested in original skyline query.
        /// </summary>
        int AllPreferencesCount { get; set; }

        /// <summary>
        ///     Get all subspaces that the skyline sampling algorithm will use to calculate its subspace skylines.
        /// </summary>
        /// <returns>
        ///     The produced subspaces. Each stored integer is an index referring the zero-based position of a preference of
        ///     the original skyline query.
        /// </returns>
        HashSet<HashSet<int>> GetSubspaces();
    }
}