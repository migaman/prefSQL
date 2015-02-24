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
        private bool _hasTOP = false;                                                   //if the query has the TOP Keyword
        private int _numberOfRecords = 0;                                               //Number of records that should be returned (0 = all)
        private List<AttributeModel> _skyline = new List<AttributeModel>();             //skyline attributes
        private List<OrderByModel> _orderBy = new List<OrderByModel>();                 //the category order by and the calculated sql
        private Dictionary<string, string> _tables = new Dictionary<string, string>();  //the tablename and its alias
        private bool _hasSkyline = false;                                               //if the query needs a skyline clause
        private SQLCommon.Ordering _ordering = SQLCommon.Ordering.AsIs;
        private bool _withIncomparable = false;                                           //variable if check for incomparable tuples is needed

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

        public bool HasSkyline
        {
            get { return _hasSkyline; }
            set { _hasSkyline = value; }
        }
        
        public bool HasTop
        {
            get { return _hasTOP; }
            set { _hasTOP = value; }
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

        public List<OrderByModel> OrderBy
        {
            set { _orderBy = value; }
            get { return _orderBy; }
        }

    }
}
