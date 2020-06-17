using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using IBM.WMQ;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;

namespace apl_back_dotnet_mq_ref.Controllers
{

    [ApiController]
    [Route("ref-back-dotnet-mq/poc")]
    public class MQTesteController: ControllerBase
    {
        private readonly IConfiguration config;
        private Hashtable qMgrProp = null;

        public MQTesteController(IConfiguration config)
        {
            this.config = config;
            
            // Obter as propriedades do MQ do arquivo appsettings.json
            qMgrProp = new Hashtable();
            qMgrProp.Add(MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_MANAGED);
            qMgrProp.Add(MQC.HOST_NAME_PROPERTY, config.GetSection("MqConnection:HostName").Value);
            qMgrProp.Add(MQC.PORT_PROPERTY, Convert.ToInt16(config.GetSection("MqConnection:Port").Value));
            qMgrProp.Add(MQC.CHANNEL_PROPERTY, config.GetSection("MqConnection:Channel").Value);
            qMgrProp.Add(MQC.CONNECT_OPTIONS_PROPERTY, MQC.MQCNO_RECONNECT_Q_MGR);
            //qMgrProp.Add(MQC.USE_MQCSP_AUTHENTICATION_PROPERTY, true);

        }

        [HttpGet("mq")]
        public ActionResult<List<string>> Consumer() 
        {
            // Definir o nome do Queue Manager e o nome Queue do MQ
            var nomeQueueManager = "nmQueueManager";
            var nomeQueue = "nmQueue";
            
            var fila = new List<string>();
            
            MQQueueManager queueManager = null;
            MQQueue queue = null;
            MQMessage mensagem = null;

            try
            {
                // Instanciar o Queue Manager e acessar a fila
                queueManager = new MQQueueManager(nomeQueueManager, qMgrProp);
                queue = queueManager.AccessQueue(nomeQueue,
                    MQC.MQOO_INPUT_AS_Q_DEF + MQC.MQOO_FAIL_IF_QUIESCING);

                // Classe contém opções que controlam o comportamento de MQQueue.get()
                var gmo = new MQGetMessageOptions();
                gmo.Options |= MQC.MQGMO_NO_WAIT | MQC.MQGMO_FAIL_IF_QUIESCING;

                // Representa o descritor de mensagens e os dados para uma mensagem do IBM MQ
                mensagem = new MQMessage {Format = MQC.MQFMT_STRING};

                // Obter a mensagem
                queue.Get(mensagem, gmo);
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                
               
                fila.Add(mensagem.ReadString(mensagem.MessageLength));

               
            }

            catch (MQException e)
            {
                Console.WriteLine(e);
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            
            finally
            {
                try
                {
                    if (queue != null)
                    {
                        queue.Close();
                        Console.WriteLine("MQTest01 closed: " + nomeQueue);
                    }
                }
                catch (MQException mqex)
                {
                    Console.WriteLine("MQTest01 CC=" + mqex.CompletionCode + " : RC=" + mqex.ReasonCode);
                }

                try
                {
                    if (queueManager != null)
                    {
                        queueManager.Disconnect();
                        Console.WriteLine("MQTest01 disconnected from " + nomeQueueManager);
                    }
                }
                catch (MQException mqex)
                {
                    Console.WriteLine("MQTest01 CC=" + mqex.CompletionCode + " : RC=" + mqex.ReasonCode);
                }
            }

            return fila;
        }


        [HttpPost("mq")]
        public ActionResult<List<string>> Producer([FromBody] string mensagem)
        {

            var retorno = new List<string>();
            
            // Definir o nome do Queue Manager e o nome Queue do MQ
            var nomeQueueManager = "nmQueueManager";
            var nomeQueue = "nmQueue";
            
            MQQueueManager queueManager = null;
            MQQueue queue = null;
            
            
            var openOptions = MQC.MQOO_OUTPUT + MQC.MQOO_FAIL_IF_QUIESCING;
            var pmo = new MQPutMessageOptions();
            

            try
            {
                queueManager = new MQQueueManager(nomeQueueManager, qMgrProp);
                //Console.WriteLine("MQTest01 successfully connected to " + qManager);

                queue = queueManager.AccessQueue(nomeQueue, openOptions);
                //Console.WriteLine("MQTest01 successfully opened " + outputQName);

                // Defina uma mensagem simples do MQ e escreva algum texto no formato UTF.
                var msg = new MQMessage();
                msg.Format = MQC.MQFMT_STRING;
                msg.MessageType = MQC.MQMT_DATAGRAM;
                msg.MessageId = MQC.MQMI_NONE;
                msg.CorrelationId = MQC.MQCI_NONE;
                msg.WriteString(mensagem);

                // Colocar a mensagem na fila
                queue.Put(msg, pmo);

                retorno.Add("Mensagem: " + mensagem + " \n incluida com sucesso !\n" + queue.ToString());
               
                
            }
            catch (MQException mqex)
            {
                Console.WriteLine("MQTest01 CC=" + mqex.CompletionCode + " : RC=" + mqex.ReasonCode);
            }
            catch (IOException ioex)
            {
                Console.WriteLine("MQTest01 ioex=" + ioex);
            }
            finally
            {
                try
                {
                    if (queue != null)
                    {
                        queue.Close();
                        Console.WriteLine("MQTest01 closed: " + nomeQueue);
                    }
                }
                catch (MQException mqex)
                {
                    Console.WriteLine("MQTest01 CC=" + mqex.CompletionCode + " : RC=" + mqex.ReasonCode);
                }

                try
                {
                    if (queueManager != null)
                    {
                        queueManager.Disconnect();
                        Console.WriteLine("MQTest01 disconnected from " + nomeQueueManager);
                    }
                }
                catch (MQException mqex)
                {
                    Console.WriteLine("MQTest01 CC=" + mqex.CompletionCode + " : RC=" + mqex.ReasonCode);
                }
            }

            return retorno;
        }
        

    }
}