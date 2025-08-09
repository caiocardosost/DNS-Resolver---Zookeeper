## Projeto de um resolvedor de dns simples usando Zookeeper

Projeto proposto na disciplina "Sistemas distribuidos" da UFABC

## Funcionamento

Basicamente o projeto consiste num resolvedor de nomes centralizado. O sistema possui 3 agentes participantes:
- ZkServer: Mecanismo central para comunicação entre os processos e armazenamento de dados
- Servidores coordenadores ou lideres: Servidores que irão processar alguma requisição (apenas um ativo por vez, escolhido por eleição)
- Servidores requisitores ou Clientes: Servidores que consultam ou registram algum serviço.
- Servidores de Log: Produz um log com o nome dos servidores registrados no sistema. 
Desta forma, elimina-se a necessidade de socket para comunicação, afinal tudo é realizado usando o Zkserver do zookeeper.

##Eleição e barreira
Em algumas situações especificas o sistema passará por um processo de eleição para definir o Lider que será o responsável por responder as requisições:
1) Na primeira inicialização do sistema.
2) O Lider atual cair

No caso 1), é necessário realizar uma eleição para saber qual servidor será escolhido como lider. Neste caso uma restrição foi estabelecida: Uma barreira que exige
a presença de pelo menos três servidores coordenadores para que a eleição prossiga. Isso pode ser entendido como uma redundância de forma a garantir uma robustez no sistema.
Os servidores que não forem eleitos colocam um Watcher no servidor lider e passam a dormir até que o evento resgistrado em seu watcher seja disparado: A queda do Lider atual.
Quando o Lider atual cair, os candidatos deveram acordar e inicializar um processo de eleição para escolher um novo líder.

##Fila
Quando um servidor requisitor é iniciado, o mesmo coloca sua requisição numa fila representada pelos filhos do znode "Queue". Este Znode é vigiado pelo servidor lider, de forma
que qualquer alteração no numero de elementos nesta "fila", o mesmo acorda e processa cada elemento desta filha (neste caso, os filhos de Queue)

##Lock
Apesar da proposta apresentada não sugerir a necessidade de Locks devido a praticamente inexistente condição de corrida, a existencia do servidor de log justifica a sua presença.
Basicamente, lider estiver realizando uma escrita de serviço no zkServer e o servidor de log tentar obter o registro de serviços neste mesmo momento, teremos uma condição de corrida. Para impedir que isso ocorra, foi adaptado Locks visando impedir que ocorra uma leitura do servidor de log enquanto uma escrita esta sendo realizada, ou uma escrita do servidor enquanto uma leitura está sendo realizada. 

## Execução

Além do código em java, o repositório conta com uma serie de scripts que contém todas as instruções necessárias para compilação e execução no windows:

`run_candLeader.bat` - Script para compilar e subir os servidores coordenadores (responsavel por responder as requisições)

`run_regServ` - Script para compilar e subir os servidores que registram de serviços

`run_resServ` - Script para compilar e subir os servidores que consultam serviços

`run_servLog.bat` - Script para compilar e subir os servidores geradores de log

Desta forma, o procedimento de execução é o seguinte:
(certifique-se de ter instalado e executado corretamente o "ZkServer". (recomendado abrir um terminal zkCli para ir verificando com "ls /" os znodes criados))

- Iniciar o arquivo `run_candLeader.bat` em 3 terminais para assim existir 3 candidatos a líder e prosseguir com a eleição
- Iniciar `run_regServ` ou `run_resServ` para realizar as requisições.
- Iniciar `run_servLog.bat` a qualquer momento para obter um registro dos servidores cadastrados.


