using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using prefSQL.SQLSkyline.Models;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
{
    /// <summary>
    /// Hexagon Algorithm implemented according to algorithm pseudocode in Preisinger (2007) and incompariblity considerations in Preisinger (2009)
    /// </summary>
    /// <remarks>
    /// Preisinger, Timotheus; Kie�ling, Werner (2007)
    /// The Hexagon Algorithm for Pareto Preference Queries.
    /// 
    /// Preisinger, Timotheus (2009)
    /// Graph-based algorithms for Pareto preference query evaluation.
    /// 
    /// Profiling considersations:
    /// - Always use equal when comparins test --> i.e. using a startswith instead of an equal can decrease performance by 10 times
    /// - Write objects from DataReader into an object[] an work with the object. 
    /// - Explicity convert (i.e. (int)reader[0]) value from DataReader and don't use the given methods (i.e. reader.getInt32(0))
    /// - Don't use DENSE_RANK() in the SQLStatement. Better to sort in C# and replace the values through ranks (method replacevaluesto...)
    /// </remarks>
    public abstract class TemplateHexagon : TemplateStrategy
    {

        protected override DataTable GetSkylineFromAlgorithm(IEnumerable<object[]> database, DataTable dataTableTemplate, string[] operatorsArray, string[] additionalParameters)
        {
            int weightHexagonIncomparable = 0;
            

            //load some variables from the additional paraemters
            if(additionalParameters.Length > 4)
            {
                //weightHexagonIncomparable = int.Parse(additionalParameters[5].Trim());
                string[] addition = additionalParameters[5].Trim(' ').Trim('\'').Split(';');
                weightHexagonIncomparable = int.Parse(addition[0]);
            }
            


            DataTable dtResult = dataTableTemplate.Clone();
            
            ArrayList[] btg = null;
            int[] next = null;
            int[] prev = null;
            int[] level = null;
            int[] weight = null;
            long maxID = 0;


            int amountOfPreferences = operatorsArray.GetUpperBound(0) + 1;


            //Replace the database values to ranks of the values
            long[] maxValues = new long[amountOfPreferences];
            database = ReplaceValuesToRankValues(database.ToList(), operatorsArray, ref maxValues);


            Construction(amountOfPreferences, maxValues, ref btg, ref next, ref prev, ref level, ref weight);


            //Read all records only once.
            foreach (object[] dbValuesObject in database)
            {
                Add(dbValuesObject, amountOfPreferences, operatorsArray, ref btg, ref weight, ref maxID, weightHexagonIncomparable);
            }

            FindBmo(amountOfPreferences, ref btg, ref next, ref prev, ref level, ref weight);

            //Now read next list
            int iItem = 0;
                
            //Ben�tigt viele Zeit im CLR-Modus (Deshalb erst hier und nur einmal initialisieren)
            //SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());

            //Until no more nodes are found
            while (iItem != -1)
            {
                //Add all records of this node
                if (btg[iItem] != null)
                {
                    //foreach (SqlDataRecord recSkyline in btg[iItem])
                    foreach (ArrayList recSkyline in btg[iItem])
                    {
                        DataRow row = dtResult.NewRow();
                        for (int i = 0; i < recSkyline.Count; i++)
                        {
                            //dataRecordTemplate.SetValue(i, recSkyline[i]);
                            row[i] = recSkyline[i];
                        }

                        dtResult.Rows.Add(row);
                    }
                }

                //Goto the next node
                iItem = next[iItem];

            }

            //TODO: Special orderings need the skyline values. Store it in a property
            //SkylineValues = resultCollection;

            return dtResult;
        }



        /// <summary>
        /// Replace the database values to ranks (this is much faster than directly with SQL, i.e. with the DENSE_RANK() function
        /// </summary>
        /// <param name="listObjects"></param>
        /// <param name="operators"></param>
        /// <param name="maxValues"></param>
        /// <returns></returns>
        private List<object[]> ReplaceValuesToRankValues(List<object[]> listObjects, string[] operators, ref long[] maxValues)
        {
            //Sort for each skyline attribute
            for (int iCol = 0; iCol <= operators.GetUpperBound(0); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (operators[iCol].Equals("LOW"))
                {
                    listObjects.Sort((object1, object2) => ((long)object1[iCol]).CompareTo((long)object2[iCol]));
                    //Now replace values beginning from 0
                    long value = (long)listObjects[0][iCol];
                    long rank = 0;
                    for (int iRow = 0; iRow < listObjects.Count; iRow++)
                    {
                        if (value < (long)listObjects[iRow][iCol])
                        {
                            value = (long)listObjects[iRow][iCol];
                            rank++;
                        }
                        listObjects[iRow][iCol] = rank;
                    }
                    //Now store the biggest rank (max value of the dimension)
                    maxValues[iCol] = rank;
                }
                else
                {
                    maxValues[iCol] = 1; //Incomparable field
                }
                
            }

            return listObjects;
        }

        private void Construction(int amountOfPreferences, long[] maxValues, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight)
        {
            int[] maxPreferenceLevel = new int[amountOfPreferences];
            for (int i = 0; i < amountOfPreferences; i++)
            {
                maxPreferenceLevel[i] = (int)maxValues[i];
            }

            //calculate edge weights
            weight = new int[amountOfPreferences];
            weight[amountOfPreferences - 1] = 1;

            for (int i = amountOfPreferences - 2; i >= 0; i--)
            {
                weight[i] = weight[i + 1] * (maxPreferenceLevel[i + 1] + 1);
            }


            // calculate the BTG size
            long sizeNodes = 1;
            for (int i = 0; i < amountOfPreferences; i++)
            {
                sizeNodes *= (maxPreferenceLevel[i] + 1);
            }                

            //Because we have 4 objects to save the tree state
            if (sizeNodes > (Int32.MaxValue / 4))
            {
                throw new Exception("Berechnung nicht m�glich mit Hexagon. Baum w�re zu gross");
            }

            btg = new ArrayList[sizeNodes];
            next = new int[sizeNodes];
            prev = new int[sizeNodes];
            level = new int[sizeNodes];

            int workSize = 1;
            for (int i = 0; i < amountOfPreferences; i++)
            {
                workSize += maxPreferenceLevel[i];
            }


            // arrays needed for init computation

            // stores highest ID found for each level by now
            int[] work = new int[workSize]; //int[max(P) + 1]

            // stores first ID found for each level by now
            int[] first = new int[workSize]; //int[max(P) + 1]

            // initialize the arrays
            next[0] = 1;

            //loop over the node IDs
            for (int id = 1; id <= sizeNodes - 1; id++)
            {
                // compute level of the node (with help of weights)
                int curLvl = 0;
                int tmpID = id; //wrong code in paper --> use of id inside de id-loop
                for (int i = 0; i <= amountOfPreferences - 1; i++)
                {
                    double dblLevel = (double)tmpID / weight[i];
                    curLvl = curLvl + (int)Math.Floor(dblLevel);
                    tmpID = tmpID - ((int)Math.Floor((dblLevel)) * weight[i]);
                }

                if (first[curLvl] == 0)
                {
                    first[curLvl] = id;
                }
                // set next node in the level
                //the current node is n�s next node and n is the current node�s previous node
                next[work[curLvl]] = id;
                prev[id] = work[curLvl];
                work[curLvl] = id; //wrong code in paper

                //For each ID, we determine to which level the corresponding node belongs
                level[id] = curLvl;
            }

            // init next relation for last nodes in the levels
            for (int curLvl = 0; curLvl < workSize - 1; curLvl++)
            {
                next[work[curLvl]] = first[curLvl + 1];

                //
                prev[first[curLvl + 1]] = work[curLvl];
            }

            //set next node of bottom node
            next[sizeNodes - 1] = -1;
            //set previous node of top node
            prev[0] = -1;
        }

        

        //Find BestMatchesOnly
        private void FindBmo(int amountPreferences, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight)
        {
            int last = 0;
            long levelUpperBound = level.GetUpperBound(0);

            // special case: top node is not empty
            if (btg[0] != null)
            {
                //Top node contains a tuple --> This is the perfect tuple. Set next to -1,
                next[0] = -1;

            }
            else
            {
                //follow the breadth-first walk
                int cur = 1;
                //while (cur != -1)
                while (cur != -1 && next[cur] != -1) //<= btg.GetUpperBound(0)
                {
                    if (btg[cur] != null)
                    {
                        // non-empty node belonging to BMO set
                        int nextCur = next[cur];
                        last = cur;
                        // remove all nodes dominated by current
                        for (int i = 0; i < amountPreferences; i++)
                        {
                            // check if there is an edge for Pi
                            if (cur + weight[i] <= level.GetUpperBound(0))
                            {
                                if (level[cur + weight[i]] == level[cur] + 1)
                                {
                                    HexagonRemoveModel modelIn = new HexagonRemoveModel(cur + weight[i], i, btg, next, prev, level, weight, 0);
                                    //removeRecursive(cur + weight[i], i, ref btg, ref next, ref prev, ref level, ref weight, 0);

                                    HexagonRemoveModel modelOut = RemoveIterative(modelIn, levelUpperBound);
                                    btg = modelOut.Btg;
                                    next = modelOut.Next;
                                    prev = modelOut.Prev;
                                    
                                }
                            }


                        }
                        cur = nextCur;

                    }
                    else
                    {
                        // node is empty: remove from next/prev
                        int nextCur = next[cur];

                        next[last] = next[cur];
                        prev[next[cur]] = last;
                        next[cur] = -1; //gibt es nicht mehr dieses node
                        prev[cur] = -1;

                        cur = nextCur; //Damit Breadt-First Walk
                    }

                }

            }



        }

        private HexagonRemoveModel RemoveIterative(HexagonRemoveModel returnModel, long levelUpperBound)
        {
            int address = 10; // Entry point for each each "call"
            HexagonRemoveModel tempModel = returnModel;            
            Stack stack = new Stack();


            stack.Push(30); //initial return address
            stack.Push(tempModel);
            while (stack.Count > 0)
            {
                switch (address)
                {
                    case 10:
                    {
                        // Do something
                        returnModel = (HexagonRemoveModel)stack.Pop();

                        int id = returnModel.ID;
                        ArrayList[] btg = returnModel.Btg;
                        int[] next = returnModel.Next;
                        int[] prev = returnModel.Prev;
                        int[] level = returnModel.Level;
                        int[] weight = returnModel.Weight;
                        
                        
                        //check if the node has already been removed
                        if (prev[id] == -1)
                        {
                            //go to next address
                            address = (int)stack.Pop();
                            stack.Push(returnModel);
                            break;
                        }
                        if (next[prev[id]] != id)
                        {
                            //go to next address
                            address = (int)stack.Pop();
                            stack.Push(returnModel);
                            break;
                        }
                        else
                        {
                            //remove the node from next/prev relation
                            next[prev[id]] = next[id];
                            if (next[id] != -1)
                            {
                                prev[next[id]] = prev[id];
                            }

                            //remove tuples in node
                            btg[id] = null;

                        }
                        


                        

                        //remove followers
                        // follow the edge for preference i (only if not already on last level)
                        //if (id + weight[0] <= level.GetUpperBound(0) && level[id + weight[0]] == level[id] + 1)
                        //TODO: Profiling separate long variable instead of GetUpperBound(0) --> No significant effect (14% from the function instead of 15%)
                        if (id + weight[0] <= levelUpperBound && level[id + weight[0]] == level[id] + 1)
                        {                               
                            //Push current object to stack
                            returnModel.Loopindex = 1;  //important: raise loop Index!! new position in position loop!!
                            returnModel.Next = next;
                            returnModel.Prev = prev;
                            returnModel.Btg = btg;
                            stack.Push(returnModel);
                            stack.Push(20);

                            //Push new object to stack
                            HexagonRemoveModel nMinus1 = new HexagonRemoveModel(id + weight[0], 0, btg, next, prev, level, weight, 0);
                            stack.Push(nMinus1);
                            address = 10; // Make another "call"
                            break;
                        }


                        
                        //Contains no childs --> goto next address
                        //The base case
                        address = (int)stack.Pop();
                        returnModel.Next = next;
                        returnModel.Prev = prev;
                        returnModel.Btg = btg;
                        stack.Push(returnModel);

                        
                        break;
                    }
                    case 20:
                    { 
                        // Compute and return
                        tempModel = (HexagonRemoveModel)stack.Pop();
                        returnModel = (HexagonRemoveModel)stack.Pop();
                        
                        //Read
                        int id = returnModel.ID;
                        int index = returnModel.Index;
                        ArrayList[] btg = returnModel.Btg;
                        int[] next = returnModel.Next;
                        int[] prev = returnModel.Prev;
                        int[] level = returnModel.Level;
                        int[] weight = returnModel.Weight;
                        int iLoopIndex = returnModel.Loopindex;


                        bool isEndOfLoop = true;

                        //remove followers
                        for (int i = iLoopIndex; i <= index; i++)
                        {
                            // follow the edge for preference i (only if not already on last level)
                            if (id + weight[i] <= level.GetUpperBound(0) && level[id + weight[i]] == level[id] + 1)
                            {
                                //Push current object to stack
                                //important: raise loop Index!! new position in position loop!! --> The next time i will be initialized by this index
                                returnModel.Loopindex = i + 1;  
                                stack.Push(returnModel);
                                stack.Push(20);

                                //Push new object to stack
                                HexagonRemoveModel deeperNode = new HexagonRemoveModel(id + weight[i], i, btg, next, prev, level, weight, 0);
                                stack.Push(deeperNode);
                                address = 10; // Make another "call"
                                isEndOfLoop = false;
                                break;
                            }
                        }

                        if (isEndOfLoop)
                        {
                            //Read next address and push current object to stack
                            address = (int)stack.Pop();
                            stack.Push(returnModel);
                        }

                        break;
                    }
                    case 30:
                    {
                        // The final return value
                        tempModel = (HexagonRemoveModel)stack.Pop();
                        break;
                    }
                }

            }
            return tempModel;
        }

        /*
        private void removeRecursive(int id, int index, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight, int iRecursionLoop)
        {
            //check if the node has already been removed
            if (prev[id] == -1)
            {
                return;
            }
            if (next[prev[id]] != id)
            {
                return;
            }
            else
            {
                //remove the node from next/prev relation
                next[prev[id]] = next[id];
                if (next[id] != -1)
                {
                    prev[next[id]] = prev[id];
                }

                //remove tuples in node
                btg[id] = null;

            }


            //remove followers
            for (int i = 0; i <= index; i++)
            {
                // follow the edge for preference i (only if not already on last level)
                if (id + weight[i] <= level.GetUpperBound(0) && level[id + weight[i]] == level[id] + 1)
                {
                    removeRecursive(id + weight[i], i, ref btg, ref next, ref prev, ref level, ref weight, ++iRecursionLoop);
                }
            }
            return;
        }
        */



        protected abstract void Add(object[] dataReader, int amountOfPreferences, string[] operators, ref ArrayList[] btg, ref int[] weight, ref long maxID, int weightHexagonIncomparable);


        
    }
}
