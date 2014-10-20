using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class PrefSQLModel
    {
        private List<AttributeModel> _skyline = new List<AttributeModel>();
        private List<String> _orderBy = new List<String>();
        private HashSet<String> _innerTableAlias = new HashSet<string>();

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
