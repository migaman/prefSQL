using System.Collections;

namespace prefSQL.SQLSkyline.Models
{
    class HexagonRemoveModel
    {
        public HexagonRemoveModel()
        {
            
        }

        public HexagonRemoveModel(int id, int index, ArrayList[] btg, int[] next, int[] prev, int[] level, int[] weight, int loopindex)
        {
            ID = id;
            Index = index;
            Btg = btg;
            Next = next;
            Prev = prev;
            Level = level;
            Weight = weight;
            Loopindex = loopindex;
        }

        public int Loopindex { get; set; }     //position in for loop
        public int ID { get; set; }
        public int Index { get; set; }
        public ArrayList[] Btg { get; set; }
        public int[] Next { get; set; }
        public int[] Prev { get; set; }
        public int[] Level { get; set; }
        public int[] Weight { get; set; }

    }
}
