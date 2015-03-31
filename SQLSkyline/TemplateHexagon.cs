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

namespace prefSQL.SQLSkyline
{
    public abstract class TemplateHexagon
    {

        public DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, String strQueryConstruction, String strConnection, string strSelectIncomparable, int weightHexagonIncomparable)
        {
            return getSkylineTable(strQuery, strOperators, numberOfRecords, strQueryConstruction, true, strConnection, strSelectIncomparable, weightHexagonIncomparable);
        }

        protected DataTable getSkylineTable(string strQuery, string strOperators, int numberOfRecords, string strQueryConstruction, bool isIndependent, string strConnection, string strSelectIncomparable, int weightHexagonIncomparable)
        {
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
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators, ref dtResult);



                //Read start of skyline
                DataTableReader sqlReader = dt.CreateDataReader();


                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    add(sqlReader, amountOfPreferences, operators, ref btg, ref weight, ref maxID, weightHexagonIncomparable);
                }
                sqlReader.Close();

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
                                    //(btg, next, prev, level, weight);
                                    HexagonRemoveModel modelIn = new HexagonRemoveModel(cur + weight[i], i, btg, next, prev, level, weight, 0);
                                    //remove(cur + weight[i], i, ref btg, ref next, ref prev, ref level, ref weight, 0);

                                    
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
            //HexagonRemoveModel first_object = null;
            int address = 10; // Entry point for each each "call"
            HexagonRemoveModel tempFac = returnModel;            
            Stack stack = new Stack();



            stack.Push(30); //initial return address
            stack.Push(tempFac);
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
                        int iLoopIndex = returnModel.loopindex;
                        
                        //
                        if(iLoopIndex == 0)
                        {
                            //check if the node has already been removed
                            if (prev[id] == -1)
                            {
                                return new HexagonRemoveModel(id, index, btg, next, prev, level, weight, 0);
                            }
                            if (next[prev[id]] != id)
                            {
                                return new HexagonRemoveModel(id, index, btg, next, prev, level, weight, 0);
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
                        }
                        else
                        {

                        }

                        if(id == 4)
                        {

                        }


                        bool bAdd = false;
                        //if (iLoopIndex < index)
                         //   bAdd = true;

                        //remove followers
                        for (int i = iLoopIndex; i <= index; i++)
                        {
                            if(i > 0 && iLoopIndex > 0 && id == 4)
                            {

                            }

                            //bAdd = i != iLoopIndex;

                            // follow the edge for preference i (only if not already on last level)
                            if (id + weight[i] <= level.GetUpperBound(0) && level[id + weight[i]] == level[id] + 1)
                            {
                                //return remove(id + weight[i], i, btg, next, prev, level, weight);
                                
                                
                                //Push current object to stack
                                returnModel.loopindex = i + 1;  //important: raise loop Index!! new position in position loop!!
                                returnModel.next = next;
                                returnModel.prev = prev;
                                returnModel.btg = btg;
                                stack.Push(returnModel);
                                stack.Push(10);

                                //Push new object to stack
                                HexagonRemoveModel nMinus1 = new HexagonRemoveModel(id + weight[i], i, btg, next, prev, level, weight, 0);
                                stack.Push(nMinus1);
                                address = 10; // Make another "call"
                                bAdd = true;
                                break;
                            }
                        }

                        if (bAdd == false)
                        {
                            //Loop erfolgreich beenden --> Nun im Stack zurückgehen und jeweils neues btg, next und prev weitergeben
                            //The base case

                            address = (int)stack.Pop();
                            stack.Push(new HexagonRemoveModel(id, index, btg, next, prev, level, weight, iLoopIndex+1));

                            /*
                            if (iLoopIndex == 0)
                            {
                                address = (int)stack.Pop();
                            }
                            else
                            {

                            }
                            */

                            //if (stack.Count > 1)
                            //{
                            //tempFac = (HexagonRemoveModel)stack.Pop();
                            
                            /*
                            returnModel = (HexagonRemoveModel)stack.Pop();
                            address = (int)stack.Pop();
                            returnModel.next = next;
                            returnModel.prev = prev;
                            returnModel.btg = btg;
                            stack.Push(returnModel);
                            */

                            /*}
                            else
                            {
                                stack.Push(returnModel);
                                address = 30;
                            }*/

                        }
                        break;
                    }
                    case 20:
                    { 
                            // Compute and return
                            

                            break;
                    }
                    case 30:
                    {
                        // The final return value
                        tempFac = (HexagonRemoveModel)stack.Pop();
                        break;
                    }
                }

            }
            return tempFac; // new HexagonRemoveModel(id, index, btg, next, prev, level, weight);
        }


        private void remove(int id, int index, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight, int iRecursionLoop)
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

            if(id == 4)
            {

            }


            //remove followers
            for (int i = 0; i <= index; i++)
            {
                // follow the edge for preference i (only if not already on last level)
                if (id + weight[i] <= level.GetUpperBound(0) && level[id + weight[i]] == level[id] + 1)
                {
                    remove(id + weight[i], i, ref btg, ref next, ref prev, ref level, ref weight, ++iRecursionLoop);
                }
            }
            return;
        }


        

        protected abstract void add(DataTableReader sqlReader, int amountOfPreferences, string[] operators, ref ArrayList[] btg, ref int[] weight, ref long maxID, int weightHexagonIncomparable);

        protected abstract void calculateOperators(ref string strOperators, string strSelectIncomparable, SqlConnection connection, ref string strSQL, ref string strQueryConstruction);
    }
}
