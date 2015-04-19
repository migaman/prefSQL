using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using prefSQL.SQLSkyline.Models;
using System.Diagnostics;

namespace prefSQL.SQLSkyline
{
    public abstract class TemplateHexagon
    {
        public long timeInMs = 0;

        public DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, String strQueryConstruction, String strConnection, string strSelectIncomparable, int weightHexagonIncomparable)
        {
            return getSkylineTable(strQuery, strOperators, numberOfRecords, strQueryConstruction, true, strConnection, strSelectIncomparable, weightHexagonIncomparable);
        }

        protected DataTable getSkylineTable(string strQuery, string strOperators, int numberOfRecords, string strQueryConstruction, bool isIndependent, string strConnection, string strSelectIncomparable, int weightHexagonIncomparable)
        {
            Stopwatch sw = new Stopwatch();
            ArrayList[] btg = null;
            int[] next = null;
            int[] prev = null;
            int[] level = null;
            int[] weight = null;
            long maxID = 0;
            DataTable dtResult = new DataTable();

            SqlConnection connection = null;
            if (isIndependent == false)
                connection = new SqlConnection(Helper.cnnStringSQLCLR);
            else
                connection = new SqlConnection(strConnection);

            String strSQL = strQuery.ToString();

            calculateOperators(ref strOperators, strSelectIncomparable, connection, ref strSQL, ref strQueryConstruction);

            string[] operators = strOperators.ToString().Split(';');
            int amountOfPreferences = operators.GetUpperBound(0) + 1;



            try
            {
                //Time the algorithm needs (afer query to the database)
                sw.Start();


                construction(amountOfPreferences, strQueryConstruction.ToString(), ref btg, ref next, ref prev, ref level, ref weight, connection);

                //Some checks
                if (strSQL.Length == Helper.MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + Helper.MaxSize);
                }
                connection.Open();

                SqlDataAdapter dap = new SqlDataAdapter(strSQL, connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);

                // Build our record schema 
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators, dtResult);



                //Read start of skyline
                DataTableReader dataTableReader = dt.CreateDataReader();


                //Write all attributes to a Object-Array
                //Profiling: This is much faster (factor 2) than working with the SQLReader
                List<object[]> listObjects = Helper.fillObjectFromDataReader(dataTableReader);

                //Read all records only once.
                foreach (object[] dbValuesObject in listObjects)
                {
                    add(dbValuesObject, amountOfPreferences, operators, ref btg, ref weight, ref maxID, weightHexagonIncomparable);
                }

                findBMO(amountOfPreferences, ref btg, ref next, ref prev, ref level, ref weight);

                //Now read next list
                int iItem = 0;
                //Benötigt viele Zeit im CLR-Modus (Deshalb erst hier und nur einmal initialisieren)
                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());

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
                                record.SetValue(i, recSkyline[i]);
                                row[i] = recSkyline[i];
                            }

                            dtResult.Rows.Add(row);
                        }
                    }

                    //Goto the next node
                    iItem = next[iItem];

                }

                //Remove certain amount of rows if query contains TOP Keyword
                Helper.getAmountOfTuples(dtResult, numberOfRecords);


                if (isIndependent == false)
                {
                    //Send results to client
                    SqlContext.Pipe.SendResultsStart(record);

                    //foreach (SqlDataRecord recSkyline in btg[iItem])
                    foreach (DataRow recSkyline in dtResult.Rows)
                    {
                        for (int i = 0; i < recSkyline.Table.Columns.Count; i++)
                        {
                            record.SetValue(i, recSkyline[i]);
                        }
                        SqlContext.Pipe.SendResultsRow(record);
                    }
                    SqlContext.Pipe.SendResultsEnd();
                }

            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineHexagon: ";
                strError += ex.Message;

                if (isIndependent == true)
                {
                    System.Diagnostics.Debug.WriteLine(strError);

                }
                else
                {
                    SqlContext.Pipe.Send(strError);
                }


            }
            finally
            {
                if (connection != null)
                    connection.Close();
            }

            sw.Stop();
            timeInMs = sw.ElapsedMilliseconds;
            return dtResult;

        }


        private void construction(int amountOfPreferences, string strQuery, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight, SqlConnection connection)
        {


            try
            {
                connection.Open();



                SqlDataAdapter dap = new SqlDataAdapter(strQuery, connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);


                int[] maxPreferenceLevel = new int[amountOfPreferences];
                for (int i = 0; i < amountOfPreferences; i++)
                {
                    maxPreferenceLevel[i] = int.Parse(dt.Rows[0][i].ToString());
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

                if (sizeNodes > System.Int32.MaxValue)
                {
                    throw new Exception("Berechnung nicht möglich mit Hexagon. Baum wäre zu gross");
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
                    //the current node is n’s next node and n is the current node’s previous node
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
                /*for (int curLvl = 1; curLvl < workSize; curLvl++)
                {
                    prev[first[curLvl]] = work[curLvl - 1];
                }*/

                //set next node of bottom node
                next[sizeNodes - 1] = -1;
                //set previous node of top node
                prev[0] = -1;

                connection.Close();

            }
            catch (Exception e)
            {
                throw e;
            }
        }

        

        //Find BestMatchesOnly
        private void findBMO(int amountPreferences, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight)
        {
            int last = 0;

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

                                    HexagonRemoveModel modelOut = modelOut = removeIterative(modelIn);
                                    btg = modelOut.btg;
                                    next = modelOut.next;
                                    prev = modelOut.prev;
                                    
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

        private HexagonRemoveModel removeIterative(HexagonRemoveModel returnModel)
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

                        int id = returnModel.id;
                        int index = returnModel.index;
                        ArrayList[] btg = returnModel.btg;
                        int[] next = returnModel.next;
                        int[] prev = returnModel.prev;
                        int[] level = returnModel.level;
                        int[] weight = returnModel.weight;
                        
                        
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
                        


                        bool containsChilds = false;

                        //remove followers
                        // follow the edge for preference i (only if not already on last level)
                        if (id + weight[0] <= level.GetUpperBound(0) && level[id + weight[0]] == level[id] + 1)
                        {                               
                            //Push current object to stack
                            returnModel.loopindex = 1;  //important: raise loop Index!! new position in position loop!!
                            returnModel.next = next;
                            returnModel.prev = prev;
                            returnModel.btg = btg;
                            stack.Push(returnModel);
                            stack.Push(20);

                            //Push new object to stack
                            HexagonRemoveModel nMinus1 = new HexagonRemoveModel(id + weight[0], 0, btg, next, prev, level, weight, 0);
                            stack.Push(nMinus1);
                            address = 10; // Make another "call"
                            containsChilds = true;
                            break;
                        }


                        if (containsChilds == false)
                        {
                            //Contains no childs --> goto next address
                            //The base case
                            address = (int)stack.Pop();
                            returnModel.next = next;
                            returnModel.prev = prev;
                            returnModel.btg = btg;
                            stack.Push(returnModel);

                        }
                        break;
                    }
                    case 20:
                    { 
                        // Compute and return
                        tempModel = (HexagonRemoveModel)stack.Pop();
                        returnModel = (HexagonRemoveModel)stack.Pop();
                        
                        //Read
                        int id = returnModel.id;
                        int index = returnModel.index;
                        ArrayList[] btg = returnModel.btg;
                        int[] next = returnModel.next;
                        int[] prev = returnModel.prev;
                        int[] level = returnModel.level;
                        int[] weight = returnModel.weight;
                        int iLoopIndex = returnModel.loopindex;


                        bool isEndOfLoop = true;

                        //remove followers
                        for (int i = iLoopIndex; i <= index; i++)
                        {
                            // follow the edge for preference i (only if not already on last level)
                            if (id + weight[i] <= level.GetUpperBound(0) && level[id + weight[i]] == level[id] + 1)
                            {
                                //Push current object to stack
                                //important: raise loop Index!! new position in position loop!! --> The next time i will be initialized by this index
                                returnModel.loopindex = i + 1;  
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

                        if (isEndOfLoop == true)
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



        protected abstract void add(object[] dataReader, int amountOfPreferences, string[] operators, ref ArrayList[] btg, ref int[] weight, ref long maxID, int weightHexagonIncomparable);

        protected abstract void calculateOperators(ref string strOperators, string strSelectIncomparable, SqlConnection connection, ref string strSQL, ref string strQueryConstruction);
    }
}
