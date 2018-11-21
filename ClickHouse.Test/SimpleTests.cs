using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ClickHouse.Test
{
    [TestClass]
    public class SimpleTests
    {
        private ClickHouseConnection GetConnection(string cstr= "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9000;Database=default;User=andreya;Password=123")
        {
            var settings = new ClickHouseConnectionSettings(cstr);
            var cnn = new ClickHouseConnection(settings);
            cnn.Open();
            return cnn;
        }

        [TestMethod]
        public void SelectIn()
        {
            using (var cnn = GetConnection())
            using (var cmd = cnn.CreateCommand("SELECT * FROM `test_data` WHERE user_id IN (@values)"))
            {
                cmd.Parameters.Add("values", DbType.UInt64, new[] {1L, 2L, 3L});
                using (var reader = cmd.ExecuteReader())
                {
                    PrintData(reader);
                }
            }

        }
        [TestMethod]
        public void SelectFromArray()
        {
            using (var cnn = GetConnection())
            using (var reader = cnn.CreateCommand("SELECT * FROM array_test").ExecuteReader())
            {
                PrintData(reader);
            }
        }

        private static void PrintData(IDataReader reader)
        {
            do
            {
                Console.Write("Fields: ");
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write("{0}:{1} ", reader.GetName(i), reader.GetDataTypeName(i));
                }
                Console.WriteLine();
                while (reader.Read())
                {
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var val = reader.GetValue(i);
                        if (val.GetType().IsArray)
                        {
                            Console.Write('[');
                            Console.Write(string.Join(", ", ((IEnumerable) val).Cast<object>()));
                            Console.Write(']');
                        }
                        else
                        {
                            Console.Write(val);
                        }
                        Console.Write(", ");
                    }
                    Console.WriteLine();
                }
                Console.WriteLine();
            } while (reader.NextResult());
        }

        [TestMethod]
        public void TestInsertNestedColumnBulk()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO nest_test (date,x, values.name,values.value)values @bulk;");
                cmd.Parameters.Add(new ClickHouseParameter
                {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[]
                    {
                        new object[] {DateTime.Now, 1, new[] {"aaaa@bbb.com", "awdasdas"}, new[] {"dsdsds", "dsfdsds"}},
                        new object[] {DateTime.Now.AddHours(-1), 2, new string[0], new string[0]},
                    }
                });
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnBulk()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO default.`super+` (date,email)values @bulk;");
                cmd.Parameters.Add(new ClickHouseParameter
                {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[]
                    {
                        new object[] {DateTime.Now,"aaaa@bbb.com"},
                        new object[] {DateTime.Now.AddHours(-1),""},
                    }
                });
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnConst()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,['a','b','c'])");
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnParam()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,@p)");
                cmd.AddParameter("p", new[] {"aaaa@bbb.com", "awdasdas"});
                cmd.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestPerfromance()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("SELECT date, time, recordId, parentId, rootId, relatedUser, successState, initiatorType, initiatorPersonSsoId, initiatorPersonToken, initiatorPersonIp, initiatorServiceServer, initiatorServiceService, initiatorServiceDN, reporterType, reporterPersonSsoId, reporterPersonToken, reporterPersonIp, reporterServiceServer, reporterServiceService, reporterServiceDN, type, parameters.name,parameters.value,objectType,objectServer,objectIdentity,objectDescription FROM dev_audit.audit_actions WHERE parentId = 0 AND date> '2017-05-22' ORDER BY time DESC LIMIT 0,10");
                cmd.AddParameter("p", new[] { "aaaa@bbb.com", "awdasdas" });
                var list = new List<List<object>>();
                var times = new List<TimeSpan>();
                var sw=new Stopwatch();
                sw.Start();
                using (var reader = cmd.ExecuteReader())
                {
                    times.Add(sw.Elapsed);
                    sw.Restart();
                    reader.ReadAll(x =>
                    {
                        var rowList = new List<Object>();
                        for (var i = 0; i < x.FieldCount; i++)
                            rowList.Add(x.GetValue(i));
                        list.Add(rowList);
                        times.Add(sw.Elapsed);
                        sw.Restart();
                    });
                }
                sw.Stop();
                
            }
        }
        [TestMethod]
        public void TestInsertFieldless()
        {
            using (var cnn = GetConnection())
            {
                var sql = "insert into vince_test values ('2017-05-17','CSA_CPTY1233',0)";
                cnn.CreateCommand(sql).ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestChecksumError()
        {
            using (var cnn = GetConnection())
            {
                var sql = "insert into vince_test(fakedate, csa, server) values('2017-05-17', 'CSA_CPTY1233', 0)";
                cnn.CreateCommand(sql).ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void InsertSelectGuid()
        {
            var newGuid = Guid.Parse( "c77a98f1-087b-4942-b9f4-2d7df360f01c");// Guid.NewGuid();
            var dbGuid = Guid.NewGuid();
            string dbGuidText = null;
            using (var cnn = GetConnection())
            {
                // Create temp table
                using (var cmd = cnn.CreateCommand("DROP TABLE IF EXISTS test_InsertSelectGuid"))
                    cmd.ExecuteNonQuery();
                using (var cmd = cnn.CreateCommand("CREATE TABLE IF NOT EXISTS test_InsertSelectGuid(date Date default today(), guid UUID) ENGINE=MergeTree(date,(date), 8192)"))
                    cmd.ExecuteNonQuery();
                // Process
                using (var cmd = cnn.CreateCommand("INSERT INTO test_InsertSelectGuid(guid) VALUES(@guid)"))
                {
                    cmd.Parameters.Add(new ClickHouseParameter
                    {
                        DbType = DbType.Guid,
                        ParameterName = "guid",
                        Value = newGuid
                    });
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = cnn.CreateCommand("SELECT guid, toString(guid) as guid_text FROM test_InsertSelectGuid"))
                using (var r = cmd.ExecuteReader())
                    r.ReadAll(rr => { dbGuid = rr.GetGuid(0); dbGuidText = rr.GetString(1); });
            }
            // Asert
            Assert.AreEqual(dbGuid.ToString(), dbGuidText, true, "Writed guid doesnt match saved.");
            Assert.AreEqual(dbGuid.ToString(), newGuid.ToString(), true, "Writed guid doesnt match read.");
        }

        public void InsertSelectGuidBulk()
        {
            var newGuid1 = Guid.Parse("c77a98f1-087b-4942-b9f4-2d7df360f01c");// Guid.NewGuid();
            var newGuid2 = Guid.NewGuid();
            var dbGuid = Guid.NewGuid();
            var l1 = new List<Guid>();
            var l2 = new List<String>();

            string dbGuidText = null;
            using (var cnn = GetConnection())
            {
                // Create temp table
                using (var cmd = cnn.CreateCommand("DROP TABLE IF EXISTS test_InsertSelectGuid"))
                    cmd.ExecuteNonQuery();
                using (var cmd = cnn.CreateCommand("CREATE TABLE IF NOT EXISTS test_InsertSelectGuid(date Date default today(), guid UUID) ENGINE=MergeTree(date,(date), 8192)"))
                    cmd.ExecuteNonQuery();
                // Process
                using (var cmd = cnn.CreateCommand("INSERT INTO test_InsertSelectGuid (guid) VALUES @bulk"))
                {
                    cmd.Parameters.Add(new ClickHouseParameter
                    {
                        DbType = DbType.Object,
                        ParameterName = "bulk",
                        Value = new[]
                        {
                            new object[] {newGuid1},
                            new object[] {newGuid2},
                        }
                    });
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = cnn.CreateCommand("SELECT guid, toString(guid) as guid_text FROM test_InsertSelectGuid"))
                using (var r = cmd.ExecuteReader())
                    r.ReadAll(rr => { l1.Add(rr.GetGuid(0)); l2.Add(rr.GetString(1)); });
            }

            // Asert
            Assert.IsTrue(l1.Count == 1, "Wrong count read.");

            Assert.IsTrue(l1.Any(t => t == newGuid1), "Writed guid1 doesnt match saved.");
            Assert.IsTrue(l1.Any(t => t == newGuid2), "Writed guid2 doesnt match saved.");

            Assert.IsTrue(l2.Any(t => t == newGuid1.ToString()), "Writed guid1 doesnt match read.");
            Assert.IsTrue(l2.Any(t => t == newGuid2.ToString()), "Writed guid2 doesnt match read.");

        }
    }
}
