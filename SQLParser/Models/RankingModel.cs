namespace prefSQL.SQLParser.Models
{
    internal class RankingModel
    {

        public RankingModel(string strFullColumnName, string strColumnName, string strExpression, double weight, string strSelectExtrema)
        {
            FullColumnName = strFullColumnName;     //Full Column name (inlcuding table name)
            ColumnName = strColumnName;             //Column Name (wihtout table name)
            Expression = strExpression;             //SQL Expression
            Weight = weight;                        //Weight of prereference
            SelectExtrema = strSelectExtrema;       //Extremas (min, max) of preference
        }

        public string SelectExtrema { get; set; }


        public double Weight { get; set; }

        public string Expression { get; set; }

        public string ColumnName { get; set; }


        public string FullColumnName { get; set; }
    }


   

}
