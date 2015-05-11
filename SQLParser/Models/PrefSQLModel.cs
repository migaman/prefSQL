using System.Collections.Generic;

namespace prefSQL.SQLParser.Models
{
    internal class PrefSQLModel
    {
        private List<AttributeModel> _skyline = new List<AttributeModel>();             //skyline preference attributes
        private List<RankingModel> _ranking = new List<RankingModel>();                 //weightedsum preference attributes
        private List<OrderByModel> _orderBy = new List<OrderByModel>();                 //the category order by and the calculated sql
        private Dictionary<string, string> _tables = new Dictionary<string, string>();  //the tablename and its alias
        

        public bool ContainsOpenPreference { get; set; }

        public PrefSQLModel()
        {
            SkylineSampleDimension = 0;
            SkylineSampleCount = 0;
            HasSkylineSample = false;
        }

        public int NumberOfRecords { get; set; }

        public bool WithIncomparable { get; set; }

        public SQLCommon.Ordering Ordering { get; set; }
        

        public Dictionary<string, string> Tables
        {
            get { return _tables; }
            set { _tables = value; }
        }

        public List<AttributeModel> Skyline
        {
            set { _skyline = value; }
            get { return _skyline; }
        }

        public List<RankingModel> Ranking
        {
            set { _ranking = value; }
            get { return _ranking; }
        }

        public List<OrderByModel> OrderBy
        {
            set { _orderBy = value; }
            get { return _orderBy; }
        }

        public int SkylineSampleCount { get; set; }
        public int SkylineSampleDimension { get; set; }
        public bool HasSkylineSample { get; set; }
        public string OriginalPreferenceSql { get; set; }
    }    
}
