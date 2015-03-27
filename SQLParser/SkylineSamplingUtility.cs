namespace prefSQL.SQLParser
{
    using System;
    using System.Collections.Generic;
    using prefSQL.SQLParser.Models;

    internal sealed class SkylineSamplingUtility
    {
        private readonly PrefSQLModel _prefSqlModel;
        private readonly SQLCommon _common;
        private static readonly Random MyRandom = new Random();

        public SQLCommon Common
        {
            get { return _common; }
        }

        public PrefSQLModel PrefSqlModel
        {
            get { return _prefSqlModel; }
        }

        public SkylineSamplingUtility(PrefSQLModel prefSqlModel, SQLCommon common)
        {
            _prefSqlModel = prefSqlModel;
            _common = common;
        }

        public string GetAnsiSql()
        {
            if (!PrefSqlModel.HasSkylineSample)
            {
                return "";
            }
            throw new System.NotImplementedException();
        }

        public List<string> GetSubspaceQueries()
        {
            if (!PrefSqlModel.HasSkylineSample)
            {
                return new List<string>();
            }

            var subspaceQueriesReturn = new Dictionary<string, List<AttributeModel>>();

            var skylineSampleCount = PrefSqlModel.SkylineSampleCount;

            var done = false;
            while (!done)
            {
                if (subspaceQueriesReturn.Count >= skylineSampleCount)
                {
                    if (AreAllPreferencesAtLeastOnceContainedInSubspaces(ref subspaceQueriesReturn))
                    {
                        done = true;
                    }
                    else
                    {
                        RemoveOneSubspaceQuery(ref subspaceQueriesReturn);
                    }
                }
                else
                {
                    AddOneSubspaceQuery(ref subspaceQueriesReturn);
                }
            }

            return new List<string>(subspaceQueriesReturn.Keys);
        }

        private bool AreAllPreferencesAtLeastOnceContainedInSubspaces(
            ref Dictionary<string, List<AttributeModel>> subspaceQueriesReturn)
        {
            var preferences = PrefSqlModel.Skyline;
            var preferencesOriginallyContainedInPrefSqlModel = new List<AttributeModel>(preferences);

            foreach (var subspaceQueryAttributes in subspaceQueriesReturn.Values)
            {
                preferencesOriginallyContainedInPrefSqlModel.RemoveAll(
                    preference => subspaceQueryAttributes.Contains(preference));
            }

            return preferencesOriginallyContainedInPrefSqlModel.Count == 0;
        }

        private static void RemoveOneSubspaceQuery(ref Dictionary<string, List<AttributeModel>> subspaceQueriesReturn)
        {
            var subspaceQueriesKeys = new List<string>(subspaceQueriesReturn.Keys);
            var subspaceQueriesKey = subspaceQueriesKeys[MyRandom.Next(subspaceQueriesKeys.Count)];
            subspaceQueriesReturn.Remove(subspaceQueriesKey);
        }

        private void AddOneSubspaceQuery(ref Dictionary<string, List<AttributeModel>> subspaceQueriesReturn)
        {
            var preferences = PrefSqlModel.Skyline;
            var preferencesOriginallyContainedInPrefSqlModel = new List<AttributeModel>(preferences);
            var skylineSampleDimension = PrefSqlModel.SkylineSampleDimension;

            string ansiSqlFromPrefSqlModel;

            do
            {
                PrefSqlModel.Skyline = new List<AttributeModel>(preferencesOriginallyContainedInPrefSqlModel);

                while (PrefSqlModel.Skyline.Count > skylineSampleDimension)
                {
                    PrefSqlModel.Skyline.RemoveAt(MyRandom.Next(PrefSqlModel.Skyline.Count));
                }

                ansiSqlFromPrefSqlModel = Common.GetAnsiSqlFromPrefSqlModel(PrefSqlModel);
            } while (subspaceQueriesReturn.ContainsKey(ansiSqlFromPrefSqlModel));

            if (ansiSqlFromPrefSqlModel != null)
            {
                subspaceQueriesReturn.Add(ansiSqlFromPrefSqlModel, new List<AttributeModel>(PrefSqlModel.Skyline));
            }

            PrefSqlModel.Skyline = new List<AttributeModel>(preferencesOriginallyContainedInPrefSqlModel);
        }
    }
}