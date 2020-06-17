# PoC - Aplicação NetCore 3.1 com IBM MQ 9

Esta aplicação de teste com NetCore 3.1, tem a finalidade de expor uma API REST para enviar ou receber mensagens do broker, IBM MQ.

## Conteúdo

- [Pré Requisitos](#pre-requisitos)
- [Casos de Uso](#casos-de-uso)
- [Trade Offs](#trade-offs)
- [Configurando sua Aplicação](#configurando-sua-aplicação)
  - [Inclusão do Pacote IBMMQDotnetClient no NuGet](#inclusão-do-pacote-ibmdotnetclient-no-nuget)
  - [Inclusão das propriedades do MQ no arquivo "app.settings.json"](#Inclusão-das-propriedades-do-MQ-no-arquivo-"app.settings.json")
  - [Carregando as Configurações do arquivo de propriedades](#Carregando-as-Configurações-do-arquivo-de-propriedades)
- [Produzindo uma mensagem](#Produzindo-uma-mensagem)
- [Consumindo uma mensagem](#consumindo-uma-mensagem)
- [Testando o Projeto Localmente](#Testando-o-Projeto-Localmente)
  - [Producer](#producer)
  - [Consumer](#consumer)
- [Configuração do DevOps do Projeto](#Configuração-do-DevOps-do-Projeto)
  - [Arquivo Dockerfile](#dockerfile)
  

## Pré Requisitos

 - SDK NetCore 3.1
 - Visual Studio Code * ou Rider ou Visual Studio 2019
 - Acesso a algum Queue Manager e Queue do IBM MQ.
 
 ## Casos de Uso
 
 - Integração entre sistemas de forma assíncrona
 - Resiliência da aplicação
 - Baixo acoplamento
 - Garantia de entrega da mensagem
 
  ## Trade Offs
  
  - Desenvolvimento mais complexo
  - Não garante a ordenação das mensagens
 
 ## Configurando sua Aplicação
 
 ### Inclusão do Pacote IBMMQDotnetClient no NuGet

A aplicação precisa entender as referências a biblitoca de abstracão com o MQ, desta forma, basta adicionar o pacote IBMMQDotnetClient na versão 9.1.5 e o namespace abaixo:

````c#
using IBM.WMQ;
````
 
 ### Inclusão das propriedades do MQ no arquivo "app.settings.json"

 Adicionar as linhas abaixo no seu arquivo "app.settings.json" da aplicação com as configurações de "Hostname", "Porta" e "Channel" do IBM MQ.
 
 ````json
"MqConnection" : {
    "HostName" : "nmHostName",
    "Port" : nrPorta,
    "Channel" : "nmChannel"
  }
````

### Carregando as Configurações do arquivo de propriedades

Incluir no seu código a interface IConfiguration com o modificador de acesso privado
  
````C#
private readonly IConfiguration config;
````

Criar um construtor da sua Classe para que o arquivo de propriedades possa ser injetado assim que esta classe for inicializada.
Estas propriedades servirão para acesso ao IBM MQ.

````c#
public MQTesteController(IConfiguration config)
{
        this.config = config;
        
        // Obter as propriedades do MQ do arquivo appsettings.json e incluí-las na Hashtable qMgrProp
        qMgrProp = new Hashtable();
        qMgrProp.Add(MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_MANAGED);
        qMgrProp.Add(MQC.HOST_NAME_PROPERTY, config.GetSection("MqConnection:HostName").Value);
        qMgrProp.Add(MQC.PORT_PROPERTY, Convert.ToInt16(config.GetSection("MqConnection:Port").Value));
        qMgrProp.Add(MQC.CHANNEL_PROPERTY, config.GetSection("MqConnection:Channel").Value);
        qMgrProp.Add(MQC.CONNECT_OPTIONS_PROPERTY, MQC.MQCNO_RECONNECT_Q_MGR);
}
````
 
 
## Produzindo uma mensagem

 O código abaixo irá gerar um produzir um evento com uma mensagem para o IBM MQ.

````c#
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
    msg.WriteString(mensagem); // variavel mensagem é a informação que deseja enviar ao MQ

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

````
 
 
## Consumindo uma mensagem
 
 O código abaixo tem o objetivo de consumir uma mensagem de uma fila no IBM MQ
 
 `````c#
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
``````

## Testando o Projeto Localmente

### Producer
Exemplo abaixo ilustrando a aplicação enviando uma mensagem para o IBM MQ.

![image](images/img_test_producer.PNG)

### Consumer

Exemplo abaixo ilustrando a aplicação consumindo uma mensagem do IBM MQ.

![image](images/img_test_consumer.PNG)


### Dockerfile

Caso o objetivo da aplicação seja executar em ambiente Linux ou MacOS, no Dockerfile do seu projeto, deve ser incluido o usúario de serviço da aplicação, para que sua aplicação containerizada possa se autenticar no IBM MQ.

````dockerfile
FROM {{ application_image_name }}:{{ application_version }}
USER root
RUN useradd -U usuarioDeServicoDoMq
ADD appsettings.json /app/out
USER usuarioDeServicoDoMq
````


