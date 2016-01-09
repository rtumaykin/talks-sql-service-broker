using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace SoCalCodeCamp
{
    class Program
    {
        private static string _connectionString;

        /// <summary>
        /// Connection string
        /// </summary>
        private static string ConnectionString
        {
            get
            {
                if (string.IsNullOrWhiteSpace(_connectionString))
                {

                    var sb = new SqlConnectionStringBuilder
                    {
                        // change the properties to match your server name and user name
                        DataSource = "db-01",
                        UserID = "foobaruser",
                        Password = "foobar123#",
                        InitialCatalog = "SoCalCodeCamp2015"
                    };

                    _connectionString = sb.ConnectionString;
                }

                return _connectionString;
            }
        }

        static void Main(string[] args)
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                while (true)
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        using (var tran = conn.BeginTransaction())
                        {
                            try
                            {
                                cmd.Transaction = tran;

                                cmd.CommandType = CommandType.StoredProcedure;

                                cmd.CommandText = "[sys].[sp_executesql]";
                                cmd.Parameters.AddWithValue("@stmt", @"
                            WAITFOR(
                                RECEIVE TOP (1) 
                                    @conversation_handle = conversation_handle,
                                    @message_type_name = message_type_name,
                                    @message_body = message_body
                                FROM [barfooqueue]
                            ), TIMEOUT 2000;");
                                cmd.Parameters.AddWithValue("@params", @"
                            @conversation_handle uniqueidentifier = NULL OUTPUT, 
                            @message_type_name nvarchar(max) = NULL OUTPUT, 
                            @message_body varbinary(max) = NULL OUTPUT");

                                cmd.Parameters.Add(new SqlParameter("@conversation_handle", SqlDbType.UniqueIdentifier,
                                    16,
                                    ParameterDirection.Output, true, 0, 0, null, DataRowVersion.Default, null));
                                cmd.Parameters.Add(new SqlParameter("@message_type_name", SqlDbType.NVarChar, -1,
                                    ParameterDirection.Output, true, 0, 0, null, DataRowVersion.Default, null));
                                cmd.Parameters.Add(new SqlParameter("@message_body", SqlDbType.VarBinary, -1,
                                    ParameterDirection.Output, true, 0, 0, null, DataRowVersion.Default, null));

                                cmd.ExecuteNonQuery();
                                var conversationHandle = (SqlGuid) cmd.Parameters["@conversation_handle"].SqlValue;

                                if (!conversationHandle.IsNull)
                                {
                                    // there is always value
                                    var messageTypeName =
                                        ((SqlString) cmd.Parameters["@message_type_name"].SqlValue).Value;

                                    if (messageTypeName == "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog")
                                    {
                                        cmd.Parameters.Clear();
                                        cmd.CommandText = "[sys].[sp_executesql]";
                                        cmd.Parameters.AddWithValue("@stmt", "END CONVERSATION (@conversation_handle);");
                                        cmd.Parameters.AddWithValue("@params", "@conversation_handle uniqueidentifier");
                                        cmd.Parameters.AddWithValue("@conversation_handle", conversationHandle.Value);
                                        cmd.ExecuteNonQuery();
                                    }

                                    if (messageTypeName == "foo")
                                    {
                                        var receivedValue =
                                            ParseReceivedMessage(
                                                ((SqlBinary) cmd.Parameters["@message_body"].SqlValue).Value);

                                        Console.WriteLine(string.Format("Received value {0}", receivedValue));

                                        cmd.Parameters.Clear();
                                        cmd.CommandText = "[sys].[sp_executesql]";
                                        cmd.Parameters.AddWithValue("@stmt",
                                            "SEND ON CONVERSATION @conversation_handle MESSAGE TYPE [bar] (@Message);");
                                        cmd.Parameters.AddWithValue("@params",
                                            "@conversation_handle uniqueidentifier, @Message xml");
                                        cmd.Parameters.AddWithValue("@conversation_handle", conversationHandle.Value);
                                        cmd.Parameters.AddWithValue("@Message",
                                            string.Format("<reply>{0}</reply>", receivedValue + 1000));
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("No values received this time");
                                }
                                tran.Commit();
                            }
                            catch (Exception x)
                            {
                                throw;
                            }
                        }
                    }
                }
            }
        }

        private static int ParseReceivedMessage(byte[] value)
        {
            var stringValue = "<root>" + Encoding.Unicode.GetString(value) + "</root>";

            var xDoc = XDocument.Parse(stringValue);

            var incomingNodeValue = xDoc.Root.Element("message").Value;
            return int.Parse(incomingNodeValue);

        }


    }
}
