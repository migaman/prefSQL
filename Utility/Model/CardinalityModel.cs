namespace Utility.Model
{
    class CardinalityModel
    {
        public CardinalityModel()
        {

        }

        public CardinalityModel(string col, int cardinality)
        {
            Col = col;
            Cardinality = cardinality;
        }

        public int Compare(object x, object y)
        {
            CardinalityModel cardinalityModelX = x as CardinalityModel;
            CardinalityModel cardinalityModelY = y as CardinalityModel;
            if (cardinalityModelX != null && cardinalityModelY != null)
            {
                return Compare(cardinalityModelX, cardinalityModelY);
            }
            else
            {
                return 0;
            }


        }

        public int Compare(CardinalityModel x, CardinalityModel y)
        {
            if (x.Cardinality > y.Cardinality)
                return -1;
            if (x.Cardinality == y.Cardinality)
                return 0;
            return 1;
        }





        public string Col { get; set; }


        public int Cardinality { get; set; }
    }
}
