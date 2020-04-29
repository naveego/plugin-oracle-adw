using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using PluginOracleADW.API.Discover;
using PluginOracleADW.API.Factory;
using PluginOracleADW.API.Read;
using PluginOracleADW.Helper;

namespace PluginOracleADW.Plugin
{
    public class Plugin : Publisher.PublisherBase
    {
        private readonly ServerStatus _server;
        private TaskCompletionSource<bool> _tcs;
        private IConnectionFactory _connectionFactory;
        private ConfigureReplicationFormData _replicationConfig;

        public Plugin(IConnectionFactory connectionFactory = null)
        {
            _connectionFactory = connectionFactory ?? new ConnectionFactory();
            _server = new ServerStatus
            {
                Connected = false,
                WriteConfigured = false,
            };
        }

        /// <summary>
        /// Establishes a connection with ADW.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>A message indicating connection success</returns>
        public override async Task<ConnectResponse> Connect(ConnectRequest request, ServerCallContext context)
        {
            Logger.SetLogPrefix("connect");
            // validate settings passed in
            try
            {
                _server.Settings = JsonConvert.DeserializeObject<Settings>(request.SettingsJson);
                _server.Settings.Validate();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = "",
                    OauthError = "",
                    SettingsError = e.Message
                };
            }

            // initialize connection factory
            try
            {
                _connectionFactory.Initialize(_server.Settings);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }

            // test cluster factory
            try
            {
                var conn =  _connectionFactory.GetConnection();
                
                await conn.OpenAsync();

                if (!await conn.PingAsync())
                {
                    return new ConnectResponse
                    {
                        OauthStateJson = request.OauthStateJson,
                        ConnectionError = "Unable to ping target database.",
                        OauthError = "",
                        SettingsError = ""
                    };
                }
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);

                return new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = e.Message,
                    OauthError = "",
                    SettingsError = ""
                };
            }

            _server.Connected = true;
            
            return new ConnectResponse
            {
                OauthStateJson = request.OauthStateJson,
                ConnectionError = "",
                OauthError = "",
                SettingsError = ""
            };
        }

        public override async Task ConnectSession(ConnectRequest request,
            IServerStreamWriter<ConnectResponse> responseStream, ServerCallContext context)
        {
            Logger.SetLogPrefix("connect_session");
            Logger.Info("Connecting session...");

            // create task to wait for disconnect to be called
            _tcs?.SetResult(true);
            _tcs = new TaskCompletionSource<bool>();

            // call connect method
            var response = await Connect(request, context);

            await responseStream.WriteAsync(response);

            Logger.Info("Session connected.");

            // wait for disconnect to be called
            await _tcs.Task;
        }


        /// <summary>
        /// Discovers schemas located in the users Zoho CRM instance
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>Discovered schemas</returns>
        public override async Task<DiscoverSchemasResponse> DiscoverSchemas(DiscoverSchemasRequest request,
            ServerCallContext context)
        {
            Logger.SetLogPrefix("discover");
            Logger.Info("Discovering Schemas...");

            var sampleSize = checked((int) request.SampleSize);

            DiscoverSchemasResponse discoverSchemasResponse = new DiscoverSchemasResponse();

            // only return requested schemas if refresh mode selected
            if (request.Mode == DiscoverSchemasRequest.Types.Mode.All)
            {
                // get all schemas
                try
                {
                    var schemas = Discover.GetAllSchemas(_connectionFactory, sampleSize);

                    discoverSchemasResponse.Schemas.AddRange(await schemas.ToListAsync());

                    Logger.Info($"Schemas returned: {discoverSchemasResponse.Schemas.Count}");

                    return discoverSchemasResponse;
                }
                catch (Exception e)
                {
                    Logger.Error(e.Message);
                    throw;
                }
            }

            try
            {
                var refreshSchemas = request.ToRefresh;

                Logger.Info($"Refresh schemas attempted: {refreshSchemas.Count}");

                var schemas = Discover.GetRefreshSchemas(_connectionFactory, refreshSchemas, sampleSize);

                discoverSchemasResponse.Schemas.AddRange(await schemas.ToListAsync());

                // return all schemas 
                Logger.Info($"Schemas returned: {discoverSchemasResponse.Schemas.Count}");
                return discoverSchemasResponse;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Publishes a stream of data for a given schema
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task ReadStream(ReadRequest request, IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            var schema = request.Schema;
            var limit = request.Limit;
            var limitFlag = request.Limit != 0;
            var jobId = request.JobId;
            var recordsCount = 0;
            
            Logger.SetLogPrefix(jobId);
            
            var records = Read.ReadRecords(_connectionFactory, schema);

            await foreach (var record in records)
            {
                // stop publishing if the limit flag is enabled and the limit has been reached or the server is disconnected
                if (limitFlag && recordsCount == limit || !_server.Connected)
                {
                    break;
                }
                
                // publish record
                await responseStream.WriteAsync(record);
                recordsCount++;
            }
            
            Logger.Info($"Published {recordsCount} records");
        }

        /// <summary>
        /// Configures replication writebacks to ADW
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ConfigureReplicationResponse> ConfigureReplication(ConfigureReplicationRequest request,
            ServerCallContext context)
        {
            Logger.Info("Configuring write...");
            //
             var schemaJson = GetSchemaJson();
             var uiJson = GetUIJson();
             
             return Task.FromResult(new ConfigureReplicationResponse
              {
                  Form = new ConfigurationFormResponse
                  {
                      DataJson = request.Form.DataJson,
                      SchemaJson = schemaJson,
                      UiJson = uiJson,
                      StateJson = request.Form.StateJson
                  }
             });
            //
            // try
            // {
            //     var errors = new List<string>();
            //     // if (! string.IsNullOrWhiteSpace(request.Form.DataJson))
            //     // {
            //     //     // check for config errors
            //     //     var replicationFormData = JsonConvert.DeserializeObject<ConfigureReplicationFormData>(request.Form.DataJson);
            //     //
            //     //     errors = Replication.ValidateReplicationFormData(replicationFormData);
            //     // }
            //     //
            //     return Task.FromResult(new ConfigureReplicationResponse
            //     {
            //         Form = new ConfigurationFormResponse
            //         {
            //             DataJson = request.Form.DataJson,
            //             Errors = {errors},
            //             SchemaJson = schemaJson,
            //             UiJson = uiJson,
            //             StateJson = request.Form.StateJson
            //         }
            //     });
            // }
            // catch (Exception e)
            // {
            //     Logger.Error(e.Message);
            //     return Task.FromResult(new ConfigureReplicationResponse
            //     {
            //         Form = new ConfigurationFormResponse
            //         {
            //             DataJson = request.Form.DataJson,
            //             Errors = {e.Message},
            //             SchemaJson = schemaJson,
            //             UiJson = uiJson,
            //             StateJson = request.Form.StateJson
            //         }
            //     });
            // }
            
            return Task.FromResult(new ConfigureReplicationResponse());
        }

        /// <summary>
        /// Prepares writeback settings to write to ADW
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task<PrepareWriteResponse> PrepareWrite(PrepareWriteRequest request, ServerCallContext context)
        {
            // Logger.SetLogPrefix(request.DataVersions.JobId);
            Logger.Info("Preparing write...");
          
            _server.WriteSettings = new WriteSettings
            {
                CommitSLA = request.CommitSlaSeconds,
                Schema = request.Schema,
                Replication = request.Replication,
                DataVersions = request.DataVersions,
            };

            if (_server.WriteSettings.IsReplication())
            {
                var conn = _connectionFactory.GetConnection();

                try
                {
                    var configSettings =
                        JsonConvert.DeserializeObject<ConfigureReplicationFormData>(request.Replication.SettingsJson);

                    _replicationConfig = configSettings;

                    await conn.OpenAsync();

                    var createTableStmt = $"create table \"{configSettings.TableName}\" ( id varchar(100), ";

                    foreach (var prop in request.Schema.Properties)
                    {
                        createTableStmt += $"\"{prop.Name}\" varchar2(500) NULL,";
                    }

                    createTableStmt +=" primary key (id))";

                    var cmd = _connectionFactory.GetCommand(createTableStmt, conn);
                    await cmd.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    Logger.Info("Could not create table: " + ex.Message);
                }
                finally
                {
                    await conn.CloseAsync();
                }
            }

            Logger.Debug(JsonConvert.SerializeObject(_server.WriteSettings, Formatting.Indented));
            Logger.Info("Write prepared.");
            _server.WriteConfigured = true;
            return new PrepareWriteResponse();
        }

        /// <summary>
        /// Writes records to ADW
        /// </summary>
        /// <param name="requestStream"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task WriteStream(IAsyncStreamReader<Record> requestStream,
            IServerStreamWriter<RecordAck> responseStream, ServerCallContext context)
        {
             try
             {
                 Logger.Info("Writing records to ADW...");
            
                 var schema = _server.WriteSettings.Schema;
                 var inCount = 0;
                 var config =
                     JsonConvert.DeserializeObject<ConfigureReplicationFormData>(_server.WriteSettings.Replication
                         .SettingsJson);
                 
                 var conn = _connectionFactory.GetConnection();
                 await conn.OpenAsync();
            
                 // get next record to publish while connected and configured
                 while (await requestStream.MoveNext(context.CancellationToken) && _server.Connected &&
                        _server.WriteConfigured)
                 {
                     var record = requestStream.Current;
                     inCount++;
            
                     Logger.Debug($"Got record: {record.DataJson}");
                     
                     if (_server.WriteSettings.IsReplication())
                     {
                         var json = JObject.Parse(record.DataJson);
                         var data = new Dictionary<string, object>();

                         var upsStmt = $"INSERT INTO \"{_replicationConfig.TableName}\" VALUES ( '{record.RecordId}', ";
                         
                         foreach (var prop in schema.Properties)
                         {
                             if (json.ContainsKey(prop.Id) && json[prop.Id] != null)
                             {
                                 upsStmt += $"'{json[prop.Id].ToString()}'";
                             }
                             else
                             {
                                 upsStmt += "null";
                             }

                             upsStmt += ",";
                         }

                         upsStmt = upsStmt.Substring(0, upsStmt.Length - 1) + ")";

                         try
                         {
                             var cmd = _connectionFactory.GetCommand(upsStmt, conn);
                             await cmd.ExecuteNonQueryAsync();
                         }
                         catch
                         {
                             
                         }

                         await responseStream.WriteAsync(new RecordAck {CorrelationId = record.CorrelationId});
                     }
                     else
                     {
                         throw new Exception("Only replication writebacks are supported");
                     }
                 }
            
                 Logger.Info($"Wrote {inCount} records to ADW.");
             }
             catch (Exception e)
             {
                 Logger.Error(e.Message);
                 throw;
             }
        }

        /// <summary>
        /// Handles disconnect requests from the agent
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<DisconnectResponse> Disconnect(DisconnectRequest request, ServerCallContext context)
        {
            // clear connection
            _server.Connected = false;
            _server.Settings = null;

            // alert connection session to close
            if (_tcs != null)
            {
                _tcs.SetResult(true);
                _tcs = null;
            }

            Logger.Info("Disconnected");
            return Task.FromResult(new DisconnectResponse());
        }
        
        public static string GetSchemaJson()
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    {"TableName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Table Name"},
                        {"description", "Name for your golden record table"},
                    }}
                }},
                {"required", new []
                {
                    "TableName"
                }}
            };

//            var schemaJsonObj = new Dictionary<string, object>();

            return JsonConvert.SerializeObject(schemaJsonObj);
        }
        
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {"ui:order", new []
                {
                    "TableName"
                }}
            };

//            var uiJsonObj = new Dictionary<string, object>();

            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}