using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class PrefSQLModel
    {
        private int _numberOfRecords = 0;                                               //Number of records that should be returned (0 = all)
        private List<AttributeModel> _skyline = new List<AttributeModel>();             //skyline preference attributes
        private List<RankingModel> _ranking = new List<RankingModel>();                 //weightedsum preference attributes
        private List<OrderByModel> _orderBy = new List<OrderByModel>();                 //the category order by and the calculated sql
        private Dictionary<string, string> _tables = new Dictionary<string, string>();  //the tablename and its alias
        private SQLCommon.Ordering _ordering = SQLCommon.Ordering.AsIs;
        private bool _withIncomparable = false;                                         //True if the skyline must be checked for incomparable tuples

        public PrefSQLModel()
        {
            SkylineSampleDimension = 0;
            SkylineSampleCount = 0;
            HasSkylineSample = false;
        }

        public int NumberOfRecords
        {
            get { return _numberOfRecords; }
            set { _numberOfRecords = value; }
        }

        public bool WithIncomparable
        {
            get { return _withIncomparable; }
            set { _withIncomparable = value; }
        }

        public SQLCommon.Ordering Ordering
        {
            get { return _ordering;  }
            set { _ordering = value;  }
        }

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
    }
}
