namespace prefSQL.SQLParser
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using prefSQL.SQLParser.Models;
    using prefSQL.SQLSkyline;

    internal sealed class SkylineSamplingUtility
    {
        private readonly PrefSQLModel _prefSqlModel;
        private readonly SQLCommon _common;
        private static readonly Random MyRandom = new Random();

        private SQLCommon Common
        {
            get { return _common; }
        }

        private PrefSQLModel PrefSqlModel
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
      
        public DataTable GetSkyline()
        {
            return Common.Helper.getSamplingResults(Common.GetAnsiSqlFromPrefSqlModel(PrefSqlModel), Common.SkylineType,
                PrefSqlModel);
        }      
    }
}
