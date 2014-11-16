using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser.Models;

namespace prefSQL.SQLParser
{
    class SQLSort
    {

        //Create the ORDERBY-Clause from the preferene model
        public string buildORDERBYClause(PrefSQLModel model, SQLCommon.OrderingType type)
        {
            string strSQL = "";
            switch (type)
            {
                case SQLCommon.OrderingType.AttributePosition:
                    strSQL = buildORDERAttributePosition(model);
                    break;
                case SQLCommon.OrderingType.RankingSummarize:
                    strSQL = buildORDERRankingSum(model);;
                    break;
                case SQLCommon.OrderingType.RankingBestOf:
                    strSQL = buildORDERRankingBestOf(model);
                    break;
                case SQLCommon.OrderingType.AsIs:
                    strSQL = ""; //Return no ORDER BY Clause
                    break;
                case SQLCommon.OrderingType.Random:
                    strSQL = buildORDERRandom(model);
                    break;
            }

            if (strSQL.Length > 0)
            {
                strSQL = " ORDER BY " + strSQL;
            }
            return strSQL;
        }


        /**
         *  Sorts the results according to the attributes values. the first attribute has the highest priority.
         *  For example a tuple has the attributes price and color. The result will be sorted after price and color, whereas 
         *  the price has the higher priority
         * 
         * */
        private string buildORDERAttributePosition(PrefSQLModel model)
        {
            string strSQL = "";
            for (int iChild = 0; iChild < model.OrderBy.Count; iChild++)
            {
                //First record doesn't need a comma to separate
                if (iChild > 0)
                {
                    strSQL += ", ";
                }
                strSQL += model.OrderBy[iChild].ToString();
            }
            return strSQL;
        }


        /**
         *  Sorts the results according to their summed ranking. 
         *  For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 13.
         * 
         * */
        private string buildORDERRankingSum(PrefSQLModel model)
        {
            string strSQL = "";


            for (int iChild = 0; iChild < model.Rank.Count; iChild++)
            {
                //First attribute doesn't need a plus
                if(iChild > 0)
                {
                    strSQL += " + ";
                }
                strSQL += model.Rank[iChild].Expression;

            }

            return strSQL;
        }


        /**
         *  Sorts the results according to their best ranking of all attributes
         *  For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 1.
         * 
         * */
        private string buildORDERRankingBestOf(PrefSQLModel model)
        {
            string strSQL = "CASE ";
            string strRanking = "";

            for (int iChild = 0; iChild < model.Rank.Count; iChild++)
            {
                if(iChild == model.Rank.Count-1)
                {
                    //Last record only needs ELSE
                    strSQL += " ELSE " + "" + model.Rank[iChild].Expression;
                }
                else
                {
                    strSQL += "WHEN ";
                    strRanking = model.Rank[iChild].Expression;
                    for (int iSubChild = iChild+1; iSubChild < model.Rank.Count; iSubChild++)
                    {
                        strSQL += strRanking + " <=" + model.Rank[iSubChild].Expression;
                        if(iSubChild < model.Rank.Count-1)
                        {
                            strSQL += " AND ";
                        }
                    }
                    strSQL += " THEN " + model.Rank[iChild].Expression + " ";
                }
                
            }
            strSQL += " END";

            return strSQL;
        }


        /**
         *  Sorts the results according to their best ranking of all attributes
         *  For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 1.
         * 
         * */
        private string buildORDERRandom(PrefSQLModel model)
        {
            string strSQL = "";
            
            //TODO: build a truly random sort function

            return strSQL;
        }
        
    }
}
