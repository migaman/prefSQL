using System.Collections;

namespace prefSQL.SQLSkyline.Models
{
    class HexagonRemoveModel
    {
        public HexagonRemoveModel()
        {
            
        }

        public HexagonRemoveModel(int _id, int _index, ArrayList[] _btg, int[] _next, int[] _prev, int[] _level, int[] _weight, int _loopindex)
        {
            id = _id;
            index = _index;
            btg = _btg;
            next = _next;
            prev = _prev;
            level = _level;
            weight = _weight;
            loopindex = _loopindex;
        }

        public int loopindex; //position in for loop
        public int id;
        public int index;
        public ArrayList[] btg { get; set; }
        public int[] next;
        public int[] prev;
        public int[] level;
        public int[] weight;

    }
}
