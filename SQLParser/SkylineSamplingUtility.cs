namespace prefSQL.SQLParser
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using prefSQL.SQLParser.Models;

    internal sealed class SkylineSamplingUtility
    {
        private readonly PrefSQLModel _prefSqlModel;
        private readonly SQLCommon _common;
        private static readonly Random MyRandom = new Random();
        private HashSet<HashSet<AttributeModel>> _subspaceQueries;

        private SQLCommon Common
        {
            get { return _common; }
        }

        private PrefSQLModel PrefSqlModel
        {
            get { return _prefSqlModel; }
        }

        private HashSet<HashSet<AttributeModel>> Subspaces
        {
            get
            {
                if (_subspaceQueries == null)
                {
                    DetermineSubspaces();
                }
                return _subspaceQueries;
            }
            set { _subspaceQueries = value; }
        }

        public HashSet<string> SubspaceQueries
        {
            get
            {
                var subspaceQueriesReturn = new HashSet<string>();

                foreach (var subspace in Subspaces)
                {
                    subspaceQueriesReturn.Add(BuildSubspaceQuery(subspace));
                }

                return subspaceQueriesReturn;
            }
        }

        public SkylineSamplingUtility(PrefSQLModel prefSqlModel, SQLCommon common)
        {
            _prefSqlModel = prefSqlModel;
            _common = common;
        }

        private void DetermineSubspaces()
        {
            Subspaces = null;

            if (!PrefSqlModel.HasSkylineSample)
            {
                Subspaces = new HashSet<HashSet<AttributeModel>>();
                return;
            }

            var subspacesReturn = new HashSet<HashSet<AttributeModel>>();

            var skylineSampleCount = PrefSqlModel.SkylineSampleCount;

            var done = false;
            while (!done)
            {
                if (subspacesReturn.Count >= skylineSampleCount)
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

        public void RedetermineSubspaces()
        {
            DetermineSubspaces();
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
            var skylineSampleDimension = PrefSqlModel.SkylineSampleDimension;
            HashSet<AttributeModel> subspacePreferencesAsHashSet;

            do
            {
                var subspacePreferences = new List<AttributeModel>(PrefSqlModel.Skyline);

                while (subspacePreferences.Count > skylineSampleDimension)
                {
                    subspacePreferences.RemoveAt(MyRandom.Next(subspacePreferences.Count));
                }

                subspacePreferencesAsHashSet = new HashSet<AttributeModel>(subspacePreferences);
            } while (subspaceQueries.Any(element => element.SetEquals(subspacePreferencesAsHashSet)));
           
            subspaceQueries.Add(subspacePreferencesAsHashSet);
        }

        public DataTable GetSkyline()
        {
            var skylineSample = new DataTable();

            foreach (var subspace in Subspaces)
            {
                Common.SkylineType.UseDataTable = null;
                var subspaceDataTable = Common.Helper.getResults(BuildSubspaceQuery(subspace), Common.SkylineType,
                    PrefSqlModel.WithIncomparable);

                var subspaceComplement = GetSubspaceComplement(subspace);
                Common.SkylineType.UseDataTable = subspaceDataTable;
                var subspaceComplementDataTable = Common.Helper.getResults(BuildSubspaceQuery(subspaceComplement),
                    Common.SkylineType, PrefSqlModel.WithIncomparable);

                skylineSample.Merge(subspaceComplementDataTable, false, MissingSchemaAction.Add);
            }

            return skylineSample;
        }

        public HashSet<AttributeModel> GetSubspaceComplement(HashSet<AttributeModel> subspace)
        {
            var allPreferences = new List<AttributeModel>(PrefSqlModel.Skyline);
            allPreferences.RemoveAll(subspace.Contains);
            return new HashSet<AttributeModel>(allPreferences);
        }
    }
}