//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{
    public abstract class SkylineStrategy
    {
        public string Provider { get; set; }

        public string ConnectionString { get; set; }

        public int WindowHandling { get; set; }

        internal TemplateStrategy Strategy { get; set; }

        /// <summary>
        /// Product of Cardinality of the preferenes
        /// </summary>
        public long Cardinality { get; set; }

        /// <summary>
        /// To define a special sort order (0=no, 1=SUM_RANK(), 2 = BEST_RANK()
        /// </summary>
        public int SortType { get; set; }

        /// <summary>
        /// Defines if there is an incomparable preference
        /// </summary>
        public bool HasIncomparablePreferences { get; set; }

        /// <summary>
        /// Add here not common parameters like for hexagon
        /// </summary>
        public string[] AdditionParameters { get; set; }

        /// <summary>
        /// Limit the tupels that will be returned
        /// </summary>
        public int RecordAmountLimit { get; set; }

        /// <summary>
        /// Only used for performance measurement
        /// </summary>
        public long NumberOfComparisons { get; set; }

        public long NumberOfMoves { get; set; }

        /// <summary>
        /// To measure the time that the algorithm needs
        /// </summary>
        public long TimeMilliseconds { get; set; }

        public int MultipleSkylineUpToLevel { get; set; }

        /// <summary>
        /// Direct call (without MS SQL CLR) to get a skyline.
        /// Additional things can be set with the properties
        /// </summary>
        /// <param name="querySQL"></param>
        /// <param name="preferenceOperators"></param>
        /// <returns></returns>
        public abstract DataTable GetSkylineTable(String querySQL, String preferenceOperators);

        /// <summary>
        /// TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="preferenceOperators"></param>
        /// <returns></returns>
        internal abstract DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators);

        public abstract String GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes);

        public abstract bool IsNative();

        //If the algorithm can hande implicit preferences like 'red' >> 'blau' without an OTHER statement
        public abstract bool SupportImplicitPreference();
        
        //If the algorithm supports incomparable values
        public abstract bool SupportIncomparable();    

        public abstract void PrepareDatabaseForAlgorithm(ref IEnumerable<object[]> useDatabase, List<int> subspace, int[] preferenceColumnIndex, bool[] isPreferenceIncomparable);
    }
}
