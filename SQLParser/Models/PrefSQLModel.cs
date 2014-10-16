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

        

        





    }
}
