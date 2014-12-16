using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class PrefSQLModel
    {
        private bool _hasTOP = false;                                                   //if the query has the TOP Keyword
        private List<AttributeModel> _skyline = new List<AttributeModel>();             //skyline attributes
        private List<string> _orderBySkyline = new List<string>();                             //orderby attributes
        private Dictionary<string, string> _orderBy = new Dictionary<string, string>();  //the category order by and the calculated sql
        private Dictionary<string, string> _tables = new Dictionary<string, string>();  //the tablename and its alias
        private bool _hasSkyline = false;                                               //if the query needs a skyline clause
        private bool _hasPrioritize = false;                                            //if the query needs a prioritize clause

        public bool HasPrioritize
        {
            get { return _hasPrioritize; }
            set { _hasPrioritize = value; }
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

        public List<string> OrderBySkyline
        {
            set { _orderBySkyline = value; }
            get { return _orderBySkyline; }
        }

        public Dictionary<string, string> OrderBy
        {
            set { _orderBy = value; }
            get { return _orderBy; }
        }

    }
}
