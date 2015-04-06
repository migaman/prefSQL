namespace prefSQL.SQLParser
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using prefSQL.SQLParser.Models;

    internal sealed class SkylineSamplingUtility
    {
        private readonly PrefSQLModel _prefSqlModel;
        private readonly SQLCommon _common;
        private static readonly Random MyRandom = new Random();
        private HashSet<HashSet<AttributeModel>> _subspacs;
        private HashSet<string> _subspaceQueries;

        private SQLCommon Common
        {
            get { return _common; }
        }

        private PrefSQLModel PrefSqlModel
        {
            get { return _prefSqlModel; }
        }

        internal HashSet<HashSet<AttributeModel>> Subspaces
        {
            get
            {
                if (_subspacs == null)
                {
                    DetermineSubspaces();
                }
                return _subspacs;
            }
            private set { _subspacs = value; }
        }

        public HashSet<string> SubspaceQueries
        {
            get
            {
                if (_subspaceQueries == null)
                {
                    _subspaceQueries = new HashSet<string>();

                    foreach (var subspace in Subspaces)
                    {
                        _subspaceQueries.Add(BuildSubspaceQuery(subspace));
                    }
                }

                return _subspaceQueries;
            }
            private set { _subspaceQueries = value; }
        }

        public SkylineSamplingUtility(PrefSQLModel prefSqlModel, SQLCommon common)
        {
            _prefSqlModel = prefSqlModel;
            _common = common;
        }

        private void DetermineSubspaces()
        {
            SubspaceQueries = null;
            Subspaces = null;

            if (!PrefSqlModel.HasSkylineSample)
            {
                Subspaces = new HashSet<HashSet<AttributeModel>>();
                return;
            }

            var subspacesCount = PrefSqlModel.SkylineSampleCount;

            var subspaceDimension = PrefSqlModel.SkylineSampleDimension;
            var skylinePreferences = PrefSqlModel.Skyline.Count;

            if (subspacesCount*subspaceDimension < skylinePreferences)
            {
                throw new Exception(
                    String.Format(
                        "Every preference has to be included in at least one subspace. This is not possible, since there are {0} preferences and at most COUNT (= {1}) * DIMENSION (= {2}) = {3} of them are included",
                        skylinePreferences, subspacesCount, subspaceDimension, subspacesCount*subspaceDimension));
            }

            var binomialCoefficient = QuickBinomialCoefficient(skylinePreferences, subspaceDimension);

            if (subspacesCount > binomialCoefficient)
            {
                throw new Exception(
                    String.Format(
                        "Cannot choose {0} from {1} in order to gain {2} subspaces, at most {3} subspaces possible.",
                        subspaceDimension, skylinePreferences, subspacesCount, binomialCoefficient));
            }

            var subspacesReturn = new HashSet<HashSet<AttributeModel>>();

            var done = false;
            while (!done)
            {
                if (subspacesReturn.Count >= subspacesCount)
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

        public void RedetermineSubspaces()
        {
            Subspaces = null;
        }

        public string GetAnsiSql()
        {
            if (!PrefSqlModel.HasSkylineSample)
            {
                return "";
            }
            throw new System.NotImplementedException();
        }

        private string BuildSubspaceQuery(IEnumerable<AttributeModel> subspace)
        {
            var allPreferences = new List<AttributeModel>(PrefSqlModel.Skyline);

            PrefSqlModel.Skyline = subspace.ToList();
            var ansiSqlFromPrefSqlModel = Common.GetAnsiSqlFromPrefSqlModel(PrefSqlModel);

            PrefSqlModel.Skyline = new List<AttributeModel>(allPreferences);

            return ansiSqlFromPrefSqlModel;
        }

        private bool AreAllPreferencesAtLeastOnceContainedInSubspaces(
            IEnumerable<HashSet<AttributeModel>> subspaceQueries)
        {
            var allPreferences = new List<AttributeModel>(PrefSqlModel.Skyline);

            foreach (var subspaceQueryPreferences in subspaceQueries)
            {
                allPreferences.RemoveAll(subspaceQueryPreferences.Contains);
            }

            return allPreferences.Count == 0;
        }

        private static void RemoveOneSubspace(ICollection<HashSet<AttributeModel>> subspaceQueries)
        {
            subspaceQueries.Remove(subspaceQueries.ElementAt(MyRandom.Next(subspaceQueries.Count)));
        }

        private void AddOneSubspace(ISet<HashSet<AttributeModel>> subspaceQueries)
        {
            HashSet<AttributeModel> subspaceQueryCandidate;

            do
            {
                subspaceQueryCandidate = new HashSet<AttributeModel>();

                while (subspaceQueryCandidate.Count < PrefSqlModel.SkylineSampleDimension)
                {
                    subspaceQueryCandidate.Add(PrefSqlModel.Skyline[MyRandom.Next(PrefSqlModel.Skyline.Count)]);
                }
            } while (subspaceQueries.Any(element => element.SetEquals(subspaceQueryCandidate)));

            subspaceQueries.Add(subspaceQueryCandidate);
        }

        public DataTable GetSkyline()
        {
            var skylineSample = new DataTable();

            foreach (var subspace in Subspaces)
            {
                var buildSubspaceQuery = BuildSubspaceQuery(subspace);
                
                Common.SkylineType.UseDataTable = null;

                var subspaceDataTable = Common.Helper.getResults(buildSubspaceQuery, Common.SkylineType,
                    PrefSqlModel);

                var columnsUsedInSubspace = ColumnsUsedInSubspace(subspaceDataTable, subspace);
                var equalRowsWithRespectToSubspaceColumnsDataTable = CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceDataTable, columnsUsedInSubspace);

                if (equalRowsWithRespectToSubspaceColumnsDataTable.Rows.Count > 0)
                {
                    var subspaceComplement = GetSubspaceComplement(subspace);
                    buildSubspaceQuery = BuildSubspaceQuery(subspaceComplement).Replace("cars_small", "#mytemptable").Replace("cars", "#mytemptable"); // TODO: obvious hack, probably extend Models

                    Common.SkylineType.UseDataTable = equalRowsWithRespectToSubspaceColumnsDataTable;
                    var subspaceComplementDataTable = Common.Helper.getResults(buildSubspaceQuery,
                        Common.SkylineType, PrefSqlModel);

                    RemoveDominatedObjects(equalRowsWithRespectToSubspaceColumnsDataTable, subspaceComplementDataTable, subspaceDataTable);
                }

                skylineSample.Merge(subspaceDataTable, false, MissingSchemaAction.Add); // TODO: does this work as expected? duplicate IDs? Common.ShowSkylineAttributes?
            }

            return skylineSample;
        }

        private static void RemoveDominatedObjects(DataTable equalRowsWithRespectToSubspaceColumnsDataTable,
            DataTable subspaceComplementDataTable, DataTable subspaceDataTable)
        {
            foreach (DataRow equalRow in equalRowsWithRespectToSubspaceColumnsDataTable.Rows)
            {
                if (IsEqualRowStillContainedWithinSubspaceComplementSkyline(subspaceComplementDataTable, equalRow))
                {
                    continue;
                }

                var remove = subspaceDataTable.Rows.Cast<DataRow>().FirstOrDefault(row => row["Id"].Equals(equalRow["Id"]));

                if (remove != null)
                {
                    subspaceDataTable.Rows.Remove(remove);
                }
            }
        }

        private static bool IsEqualRowStillContainedWithinSubspaceComplementSkyline(DataTable subspaceComplementDataTable, DataRow equalRow)
        {
            return subspaceComplementDataTable.AsEnumerable().Any(row => row["Id"].Equals(equalRow["Id"]));
        }

        private static HashSet<string> ColumnsUsedInSubspace(DataTable subspaceDataTable, HashSet<AttributeModel> subspace)
        {
            var columnsUsedInSubspace = new HashSet<string>();
            foreach (var column in subspaceDataTable.Columns)
            {
                foreach (AttributeModel attribute in subspace)
                {
                    if (attribute.FullColumnName.EndsWith(column.ToString(), true, CultureInfo.InvariantCulture))
                        // TODO: replace EndsWith, probably extend Models
                    {
                        columnsUsedInSubspace.Add(column.ToString());
                        break;
                    }
                }
            }
            return columnsUsedInSubspace;
        }

        private static DataTable CompareEachRowWithRespectToSubspaceColumnsPairwise(DataTable subspaceDataTable,
            HashSet<string> columnsUsedInSubspace)
        {
            var equalRowsWithRespectToSubspaceColumnsDataTable = subspaceDataTable.Clone();

            var equalRowsWithRespectToSubspaceColumns = new HashSet<DataRow>();

            for (var i = 0; i < subspaceDataTable.Rows.Count; i++)
            {
                for (var j = i + 1; j < subspaceDataTable.Rows.Count; j++)
                {
                    if (columnsUsedInSubspace.All(
                        item => subspaceDataTable.Rows[i][item].Equals(subspaceDataTable.Rows[j][item])))
                    {
                        if (!equalRowsWithRespectToSubspaceColumns.Contains(subspaceDataTable.Rows[i]))
                        {
                            equalRowsWithRespectToSubspaceColumnsDataTable.ImportRow(subspaceDataTable.Rows[i]);
                            equalRowsWithRespectToSubspaceColumns.Add(subspaceDataTable.Rows[i]);
                        }
                        if (!equalRowsWithRespectToSubspaceColumns.Contains(subspaceDataTable.Rows[j]))
                        {
                            equalRowsWithRespectToSubspaceColumnsDataTable.ImportRow(subspaceDataTable.Rows[j]);
                            equalRowsWithRespectToSubspaceColumns.Add(subspaceDataTable.Rows[j]);
                        }
                    }
                }
            }

            return equalRowsWithRespectToSubspaceColumnsDataTable;
        }

        public HashSet<AttributeModel> GetSubspaceComplement(HashSet<AttributeModel> subspace)
        {
            var allPreferences = new List<AttributeModel>(PrefSqlModel.Skyline);
            allPreferences.RemoveAll(subspace.Contains);
            return new HashSet<AttributeModel>(allPreferences);
        }

        private class MyDataRowEqualityComparator : IEqualityComparer<DataRow>
        {
            private readonly HashSet<string> _attrib;

            public MyDataRowEqualityComparator(HashSet<string> attrib)
            {
                _attrib = attrib;
            }

            private IEnumerable<string> Attrib
            {
                get { return _attrib; }
            }        

            public bool Equals(DataRow x, DataRow y)
            {
                return Attrib.All(attrib => x[attrib].Equals(y[attrib]));
            }

            public int GetHashCode(DataRow obj)
            {
                // http://stackoverflow.com/questions/263400/what-is-the-best-algorithm-for-an-overridden-system-object-gethashcode/263416#263416
               // unchecked // Overflow is fine, just wrap
               // {
                    var hashCode = Attrib.Aggregate(486187739, (accumulated, next) => accumulated * 29 + next.GetHashCode());
                    return hashCode;
               // }               
            }
        }
    }
}
