using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class PrefSQLModel
    {
        private String _tableName = "";
        private bool _includesTOP = false;
        private String _tableAliasName = "";
        private List<AttributeModel> _skyline = new List<AttributeModel>();
        private List<RankModel> _rank = new List<RankModel>();

        internal List<RankModel> Rank
        {
            get { return _rank; }
            set { _rank = value; }
        }
        private List<String> _orderBy = new List<String>();
        private HashSet<String> _tables = new HashSet<string>();
        private String _sql = "";



        public bool IncludesTOP
        {
            get { return _includesTOP; }
            set { _includesTOP = value; }
        }
        

        public String TableAliasName
        {
            get { return _tableAliasName; }
            set { _tableAliasName = value; }
        }
        

        public String Sql
        {
            get { return _sql; }
            set { _sql = value; }
        }

        public HashSet<String> Tables
        {
            get { return _tables; }
            set { _tables = value; }
        }
        private HashSet<String> _innerTableAlias = new HashSet<string>();

        public String TableName
        {
            get { return _tableName; }
            set { _tableName = value; }
        }

        public List<AttributeModel> Skyline
        {
            set { _skyline = value; }
            get { return _skyline; }
        }


        public List<String> OrderBy
        {
            set { _orderBy = value; }
            get { return _orderBy; }
        }

        
        public HashSet<String> InnerTableAlias
        {
            set { _innerTableAlias = value; }
            get { return _innerTableAlias; }
        }
        





    }
}
