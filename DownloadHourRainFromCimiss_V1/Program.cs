using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using cma.cimiss.client;
using cma.cimiss;
using CaoYong.Constants;
using CaoYong.DataProc;
using CaoYong.DataType;
using CaoYong.SimpleLog;
using System.Diagnostics;
using System.IO;

namespace DownloadHourRainFromCimiss_V1
{
    class Program
    {
        static void Main(string[] args)
        {
            ///////////////////////////////////////////////////////////////////////////////
            //介绍性开头
            Console.WriteLine("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            Console.WriteLine("+++    Download Hour Rain From CmissServer V1.0    +++");
            Console.WriteLine("+++++  Supported By CaoYong 2018.08.29       +++++++++");
            Console.WriteLine("+++++  QQ: 403637605                         +++++++++");
            Console.WriteLine("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            ///////////////////////////////////////////////////////////////////////////////

            ///////////////////////////////////////////////////////////////////////////////
            //打开计时器
            Stopwatch sw = new Stopwatch();  //创建计时器
            sw.Start();                      //开启计数器
            ///////////////////////////////////////////////////////////////////////////////

            ///////////////////////////////////////////////////////////////////////////////
            ///通用设置
            string appDir = System.AppDomain.CurrentDomain.SetupInformation.ApplicationBase;                                                                              //程序启动文件夹
            //string appDir = @"E:/Test/DownloadHourRainCimissData_V1/";                                                                                                  //程序启动文件夹测试
            Environment.CurrentDirectory = appDir;                                                                                                                        //设置shell启动文件夹
            string logPath = StringProcess.DateReplace(appDir + "log/YYYYMMDD.txt", DateTime.Now, 000);                                                                   //日志文件夹地址
            Log simpleLog = new Log(logPath);                                                                                                                             //建立log对象，用于日志的记录                                                                                                                              //输出站点ID计算信息
            ///////////////////////////////////////////////////////////////////////////////

            ///////////////////////////////////////////////////////////////////////////////
            try
            {
                ///////////////////////////////////////////////////////////////////////////
                //时间处理(北京时)
                DateTime dtNow = DateTime.Now;                         //程序启动时间（北京时）                          
                if (args.Length == 0)                                  //实时运算处理
                {
                    dtNow = DateTime.Now;                              //实际运行
                    //dtNow = new DateTime(2018, 12, 06, 10, 05, 00);  //测试运行
                }
                else if (args.Length == 1 && args[0].Length == 12)     //指定日期运算处理
                {
                    try
                    {
                        dtNow = DateTime.ParseExact(args[0], "yyyyMMddHHmm", System.Globalization.CultureInfo.CurrentCulture);
                    }
                    catch
                    {
                        simpleLog.WriteError("Date Args Content Is Not Right!", 1);
                        return;
                    }
                }
                else
                {
                    simpleLog.WriteError("Date Args Number Is Not Right!", 1);
                    return;
                }
                ///////////////////////////////////////////////////////////////////////////////

                ///////////////////////////////////////////////////////////////////////////////
                //读取控制文件
                string paraFilePath = appDir + @"para/para.ini";                  //控制文件地址
                string r01hStaSamplePath = null;                                  //站点数据保存地址(小时降水)
                if (!File.Exists(paraFilePath))
                {
                    simpleLog.WriteError("Para File Is Not Exist!", 1);
                    return;
                }
                else
                {
                    FileStream paraFS = new FileStream(paraFilePath, FileMode.Open, FileAccess.Read);
                    StreamReader paraSR = new StreamReader(paraFS, Encoding.GetEncoding("gb2312"));
                    {
                        try
                        {
                            string strTmp = paraSR.ReadLine();
                            string[] strArrayTmp = strTmp.Split(new char[] { '=' });
                            r01hStaSamplePath = strArrayTmp[1].Trim();                         //站点数据保存地址
                        }
                        catch
                        {
                            simpleLog.WriteError("Para Content Is Not Right!", 1);
                            return;
                        }
                    }
                    paraSR.Close();
                    paraFS.Close();
                }
                ///////////////////////////////////////////////////////////////////////////////

                ///////////////////////////////////////////////////////////////////////////////
                //下载小时级站点降水
                {
                    Console.WriteLine("Step2: Download Hour Pre Data...");
                    DataQueryClient client = new DataQueryClient();
                    string userId = ConCmiss.USERID;
                    string pwd = ConCmiss.PWD;
                    string interfaceId = "getSurfEleByTime";
                    Dictionary<String, String> paramsData = new Dictionary<String, String>
                    {
                        { "dataCode", "SURF_CHN_MUL_HOR" },             // 资料代码
                        { "elements", "Station_Id_d,Lon,Lat,PRE_1h" },  // 检索要素：站号、小时降水                                          
                        { "orderby", "Station_Id_d:ASC" },              // 排序：按照站号从小到大
                        { "times", "20180831030000" }                   // 检索时间       
                    };
                    RetArray2D rddata = new RetArray2D();
                    client.initResources();
                    DateTime dtBase = new DateTime(dtNow.Year, dtNow.Month, dtNow.Day, dtNow.Hour, 00, 00).AddHours(-8);  //下载数据基准时间,并转化为世界时
                    for (int beforeMin = 0; beforeMin <= 60 * 24; beforeMin = beforeMin + 60)    //冗余设置，下载过去24小时内的逐1小时降水
                    {
                        DateTime dtNeed = dtBase.AddMinutes(-1.0 * beforeMin);                  //所需下载数据所在时间
                        paramsData["times"] = dtNeed.ToString("yyyyMMddHHmm00");                //更新下载参数中时间
                        string outputPath = StringProcess.DateReplace(r01hStaSamplePath, dtNeed, 000);
                        if (File.Exists(outputPath))  //如果文件存在则不予下载
                        {
                            continue;
                        }
                        client.callAPI_to_array2D(userId, pwd, interfaceId, paramsData, rddata);
                        List<PointData> ltOutput = new List<PointData>();
                        for (int n = 0; n < rddata.data.GetLength(0); n++)
                        {
                            ltOutput.Add(new PointData(rddata.data[n][0].Trim(), double.Parse(rddata.data[n][1]), double.Parse(rddata.data[n][2]), double.Parse(rddata.data[n][3])));
                        }
                        ScatterData sdOutput = new ScatterData(ltOutput.ToArray());
                        sdOutput.ClearToNumGreaterThan(0.0, 100.0);        //简单质量控制
                        sdOutput.ClearToNumLessThan(0.0, 0.0);
                        if (sdOutput.StaData.Length >= 55000)              //如果数据量超过5500条，则认为数据已经完整，则写出数据，否则不予写出，配合更新数据        
                        {
                            string strHeader = StringProcess.DateReplace("diamond 3 YYYY年MM月DD日HH时NN分_逐01小时降水 YY MM DD HH  -1 0 1 0 0", dtNeed, 000);
                            sdOutput.WriterToMicaps3(outputPath, strHeader);
                        }
                        Console.Write("\rfinish {0,8:f2} %", 100.0 * (beforeMin) / (60 * 24));
                    }
                    client.destroyResources();
                    Console.WriteLine("\rfinish ok!                              ");
                }
                ///////////////////////////////////////////////////////////////////////////////

            }
            catch (Exception ex)
            {
                simpleLog.WriteError(ex.Message, 1);
            }
            ///////////////////////////////////////////////////////////////////////////////

            ///////////////////////////////////////////////////////////////////////////////
            sw.Stop();
            simpleLog.WriteInfo("Time Elasped: " + sw.Elapsed, 1);
            ///////////////////////////////////////////////////////////////////////////////
        }
    }
}
