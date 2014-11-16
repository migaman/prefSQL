using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class PrefSQLModel
    {
        private string _tableName = "";
        private bool _includesTOP = false;
        private string _tableAliasName = "";
        private List<AttributeModel> _skyline = new List<AttributeModel>();
        private List<RankModel> _rank = new List<RankModel>();
        private List<string> _orderBy = new List<string>();
        private HashSet<string> _tables = new HashSet<string>();
        private string _sql = "";
        private bool _hasSkyline = false;
        private bool _hasPrioritize = false;

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
        

        internal List<RankModel> Rank
        {
            get { return _rank; }
            set { _rank = value; }
        }




        public bool IncludesTOP
        {
            get { return _includesTOP; }
            set { _includesTOP = value; }
        }


        public string TableAliasName
        {
            get { return _tableAliasName; }
            set { _tableAliasName = value; }
        }


        public string Sql
        {
            get { return _sql; }
            set { _sql = value; }
        }

        public HashSet<string> Tables
        {
            get { return _tables; }
            set { _tables = value; }
        }
        private HashSet<string> _innerTableAlias = new HashSet<string>();

        public string TableName
        {
            get { return _tableName; }
            set { _tableName = value; }
        }

        public List<AttributeModel> Skyline
        {
            set { _skyline = value; }
            get { return _skyline; }
        }


        public List<string> OrderBy
        {
            set { _orderBy = value; }
            get { return _orderBy; }
        }


        public HashSet<string> InnerTableAlias
        {
            set { _innerTableAlias = value; }
            get { return _innerTableAlias; }
        }
        





    }
}
