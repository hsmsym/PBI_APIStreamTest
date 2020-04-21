using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace PBI_APIStreamTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //sendRandom();
            sendTLog();
        }

        static void sendRandom()
        {
            string jsonDt;
            string timeFormat = "yyyy-MM-dd'T'HH:mm:ss.fffK";
            HttpClient client = new HttpClient();

            while (1 == 1)
            {
                //timeFormat = "yyyy’-‘MM’-‘dd’T’HH’:’mm’:’ss.fffK";
                int randomSpeed;
                System.Random generator = new System.Random();

                randomSpeed = generator.Next(10, 50);
                jsonDt = "[{\"Speed\":" + randomSpeed.ToString();
                jsonDt = jsonDt + ",\"DateTime\":\"" + DateTime.UtcNow.ToString(timeFormat);
                jsonDt = jsonDt + "\"}]";
                HttpContent content = new StringContent(jsonDt);
                Uri url = new Uri("https://api.powerbi.com/beta/3026e9ae-1c6f-48bc-aa40-711484d97639/datasets/270f9cdb-f67d-469e-b8c0-013cb5074061/rows?noSignUpCheck=1&key=tVFPVHJltMTtis8xkmvcxvsFJVMIltf78wTMamYG%2B%2FWJ6NzARljznxer6JmLBKAqq1orpFn%2B%2FJ3YikjFohlB2w%3D%3D");
                HttpResponseMessage response;
                response = client.PostAsync(url, content).Result;
                System.Threading.Thread.Sleep(2000);
                Console.WriteLine(jsonDt.ToString());
            }
        }

        static void sendTLog()
        {
            string jsonstr;
            string timeFormat = "yyyy-MM-dd'T'HH:mm:ss.fffK";
            HttpClient client = new HttpClient();
            /*
              [
                {
                "LogID" :"AAAAA555555",
                "InterfaceID" :"AAAAA555555",
                "StartTime" :"2020-04-14T08:22:11.441Z",
                "RecordCount" :98.6,
                "ProcessStatus" :"AAAAA555555",
                "FinishTime" :"2020-04-14T08:22:11.441Z"
                }
              ]
            */

            while (true)
            {
                // Sql 연결정보
                string connectionString = "server = 192.168.6.25; uid = DDBTDEV2; pwd = daeduck!1; database = CircleLogicBTSMgrDB;";
                // Sql 새연결정보 생성
                SqlConnection sqlConn = new SqlConnection(connectionString);
                SqlCommand sqlComm = new SqlCommand();
                sqlComm.Connection = sqlConn;
                sqlComm.CommandText = @"select top 1 
                                              [LogID]
                                              ,[InterfaceID]
                                              ,[StartTime]
                                              ,[RecordCount]
                                              ,[ProcessStatus]
                                              ,[FinishTime]
                                        FROM [CircleLogicBTSMgrDB].[dbo].[T_LogMaster]
                                        order by FinishTime desc";
                //sqlComm.Parameters.AddWithValue("@param1", "김준");
                sqlConn.Open();
                
                string logId, interfaceId, processStatus;
                DateTime startTime, finishTime;
                int recordCount;

                using (SqlDataReader SqlRs = sqlComm.ExecuteReader())
                {
                    SqlRs.Read(); // 하나만 읽음
                    logId = SqlRs.GetString(0);
                    interfaceId = SqlRs.GetString(1);
                    startTime = SqlRs.GetDateTime(2);
                    recordCount = SqlRs.GetInt32(3);
                    processStatus = SqlRs.GetString(4);
                    finishTime = SqlRs.GetDateTime(5);
                }
                sqlConn.Close();

                jsonstr = "[{";
                jsonstr = jsonstr + "\"LogID\" : \"" + logId + "\"";
                jsonstr = jsonstr + ",\"InterfaceID\" : \"" + interfaceId + "\"";
                jsonstr = jsonstr + ",\"StartTime\" : \"" + startTime.ToString(timeFormat) + "\"";
                jsonstr = jsonstr + ",\"RecordCount\" : " + recordCount + "";
                jsonstr = jsonstr + ",\"ProcessStatus\" : \"" + processStatus + "\"";
                jsonstr = jsonstr + ",\"FinishTime\" : \"" + finishTime.ToString(timeFormat) + "\"";
                jsonstr = jsonstr + "}]";

                Console.WriteLine(jsonstr.ToString());
        
                HttpContent content = new StringContent(jsonstr);
                Uri url = new Uri("https://api.powerbi.com/beta/3026e9ae-1c6f-48bc-aa40-711484d97639/datasets/bf04e471-85da-43e3-ad48-a6d2a1ae8f21/rows?key=WuvS0uF7FfoioJV4SlsWwaj%2FhCz%2FZzHkqwyb7U9%2Fvj%2Fu6u4MpsTkU%2BR2qvfpr5RgQ4ohyQ4sCHrfwQJkyvOA4w%3D%3D");
                HttpResponseMessage response;
                response = client.PostAsync(url, content).Result;

                if (response.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    throw new Exception(response.Content.ToString());
                }
                System.Threading.Thread.Sleep(5000); // 5초
            }
        }
    }
}
