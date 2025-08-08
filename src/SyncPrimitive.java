import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/* Oq SyncPrimitive faz?
Estabelece uma conexão com o ZooKeeper (se ainda não existir).

Define a classe como um "Watcher", ou seja, um observador de eventos do ZooKeeper.

Usa um mutex para permitir que threads locais possam esperar por eventos do ZooKeeper, como mudanças em um znode.

Quando um evento ocorre, process() é chamado, e uma thread bloqueada é acordada com notify()
*/

public class SyncPrimitive implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;
    private static CountDownLatch latch = new CountDownLatch(1);

    String root;

    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = Integer.valueOf(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
        //else mutex = Integer.valueOf(-1);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }

    /**
     * Barrier
     */
	 
/* O que esse trecho faz?
 Estabelece a conexão com o ZooKeeper
 Cria (se necessário) o znode raiz da barreira no ZooKeeper
 Define um nome único para o processo local
 Prepara o ambiente para que esse processo possa entrar em uma barreira distribuída com outros processos*/
 
    static public class Barrier extends SyncPrimitive {
        int size;
        String name;

        /**
         * Barrier constructor
         *
         * @param address
         * @param root
         * @param size
         */
        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;

            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }

            // My node name
            try {
                name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
            } catch (UnknownHostException e) {
                System.out.println(e.toString());
            }

        }

        /**
         * Join barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
/*Esse método faz com que o processo:

Se registre na barreira criando um znode efêmero sequencial.

Espere até que todos os size processos tenham chegado à barreira.

Só continua (retorna true) quando todos estiverem presentes.*/

        boolean enter() throws KeeperException, InterruptedException{
            zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);

                    if (list.size() < size) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }

        /**
         * Wait until all reach barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
		 
/*Esse método faz com que o processo:

Delete seu znode na barreira,

Espere até que todos os processos tenham saído,

E só então continue. */

        boolean leave() throws KeeperException, InterruptedException{
            zk.delete(root + "/" + name, 0);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                        if (list.size() > 0) {
                            mutex.wait();
                        } else {
                            return true;
                        }
                    }
                }
        }
    }

    /**
     * Producer-Consumer queue
     */
    static public class Queue extends SyncPrimitive {

        /**
         * Constructor of producer-consumer queue
         *
         * @param address
         * @param name
         */
        Queue(String address, String name) {
            super(address);
            this.root = name;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }

        /**
         * Add element to the queue.
         *
         * @param i
         * @return
         */

        boolean produce(int i) throws KeeperException, InterruptedException{
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;

            // Add child with value i
            b.putInt(i);
            value = b.array();
            zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }


        /**
         * Remove first element from the queue.
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        int consume() throws KeeperException, InterruptedException{
            int retvalue = -1;
            Stat stat = null;

            // Get the first element available
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
                        System.out.println("Going to wait");
                        mutex.wait();
                    } else {
                        Integer min = Integer.valueOf(list.get(0).substring(7));
                        System.out.println("List: "+list.toString());
                        String minString = list.get(0);
                        for(String s : list){
                            Integer tempValue = Integer.valueOf(s.substring(7));
                            //System.out.println("Temp value: " + tempValue);
                            if(tempValue < min) { 
                            	min = tempValue;
                            	minString = s;
                            }
                        }
                       System.out.println("Temporary value: " + root +"/"+ minString);
                        byte[] b = zk.getData(root +"/"+ minString,false, stat);
                        //System.out.println("b: " + Arrays.toString(b)); 	
                        zk.delete(root +"/"+ minString, 0);
                        ByteBuffer buffer = ByteBuffer.wrap(b);
                        retvalue = buffer.getInt();
                        return retvalue;
                    }
                }
            }
        }
    }

    static public class Lock extends SyncPrimitive {
    	long wait;
	String pathName;
    	 /**
         * Constructor of lock
         *
         * @param address
         * @param name Name of the lock node
         */
        Lock(String address, String name, long waitTime) {
            super(address);
            this.root = name;
            this.wait = waitTime;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }
        
        boolean lock() throws KeeperException, InterruptedException{
            //Step 1
            pathName = zk.create(root + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("My path name is: "+pathName);
            //Steps 2 to 5
            return testMin();
        }
        
	boolean testMin() throws KeeperException, InterruptedException{
	    while (true) {
		 Integer suffix = Integer.valueOf(pathName.substring(12));
	         //Step 2 
            	 List<String> list = zk.getChildren(root, false);
                 Integer min = Integer.valueOf(list.get(0).substring(5));
                 System.out.println("List: "+list.toString());
                 String minString = list.get(0);
                 for(String s : list){
                 	Integer tempValue = Integer.valueOf(s.substring(5));
                 	//System.out.println("Temp value: " + tempValue);
                 	if(tempValue < min)  {
                 		min = tempValue;
                 		minString = s;
                 	}
                 }
                System.out.println("Suffix: "+suffix+", min: "+min);
           	//Step 3
             	if (suffix.equals(min)) {
            		System.out.println("Lock acquired for "+minString+"!");
            		return true;
            	}
            	//Step 4
            	//Wait for the removal of the next lowest sequence number
            	Integer max = min;
            	String maxString = minString;
            	for(String s : list){
            		Integer tempValue = Integer.valueOf(s.substring(5));
            		//System.out.println("Temp value: " + tempValue);
            		if(tempValue > max && tempValue < suffix)  {
            			max = tempValue;
            			maxString = s;
            		}
            	}
            	//Exists with watch
            	Stat s = zk.exists(root+"/"+maxString, this);
            	System.out.println("Watching "+root+"/"+maxString);
            	//Step 5
            	if (s != null) {
            	    //Wait for notification
            	    break;  
            	}
	    }
            System.out.println(pathName+" is waiting for a notification!");
	    return false;
	}

        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
            	String path = event.getPath();
            	if (event.getType() == Event.EventType.NodeDeleted) {
            		System.out.println("Notification from "+path);
			try {
			    if (testMin()) { //Step 5 (cont.) -> go to step 2 to check
				this.compute();
			    } else {
				System.out.println("Not lowest sequence number! Waiting for a new notification.");
			    }
			} catch (Exception e) {e.printStackTrace();}
            	}
            }
        }
        
        void compute() {
        	System.out.println("Lock acquired!");
    		try {
				new Thread().sleep(wait);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		//Exits, which releases the ephemeral node (Unlock operation)
    		System.out.println("Lock released!");
    		System.exit(0);
        }
    }
    
    static public class Leader extends SyncPrimitive {
    	
    	/*construtor - Cria um Znode "/Dns" na raiz caso não exista*/
    	
    	Leader(String address, String root){
    		super(address);
            this.root = root;
            
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
    	}
    	
    	void leaderProcess() throws KeeperException, InterruptedException {
    		String queuePathName = "/Queue";
    		String servicePathName = "/Service";
    		Stat queueExist = zk.exists(queuePathName, false);
    		if (queueExist == null) {
    			zk.create(queuePathName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    		}
    		Stat serviceExist = zk.exists(servicePathName, false);
    		if (serviceExist == null) {
    			zk.create(servicePathName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    		}
    		System.out.println("Pronto para processar requisições!!");
    		while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(queuePathName, true);
                    if (list.size() == 0) {
                        //dormindo se não existem requisições
                    	System.out.println("Esperando novas requisições!! zzzzZZZZ");
                    	while(list.size()==0) {
                    		mutex.wait();
                    		list = zk.getChildren(queuePathName, true);
                    	}
                        
                    } else {
                    	System.out.println("Novas Requisições Disponiveis!");
                    	System.out.println("Fila de requisições: ");
                        Integer min = Integer.valueOf(list.get(0).substring(8));
                        System.out.println("List: "+list.toString());
                        String minString = list.get(0);
                        for(String s : list){
                            Integer tempValue = Integer.valueOf(s.substring(8));
                            //System.out.println("Temp value: " + tempValue);
                            if(tempValue < min) { 
                            	min = tempValue;
                            	minString = s;
                            }
                        }
                        System.out.println("Processando requisição "+ minString);
                        //recuperando a informação do znode "min" de Queue:
                        byte[] dataB = zk.getData(queuePathName+"/"+minString, false, null);
                        // Converte os bytes de volta para String
                        String dataS = new String(dataB, StandardCharsets.UTF_8);
                        // Divide a string original de volta em um array de strings:
                        String[] data = dataS.split(";");
                        if(data[0].equals("reg")) {
                        	String pathName;                   		 
                    		byte[] ip = data[2].getBytes(StandardCharsets.UTF_8);
                    		pathName = zk.create(servicePathName + "/" + data[1], ip, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    		new Thread().sleep(5000);
                    		System.out.println("Concluido!");
                    		String message = "done!";
                    		byte[] messageB = message.getBytes(StandardCharsets.UTF_8);
                    		zk.setData(data[3],messageB, -1);
                    		
                        } else {
                        	List<String> list_2 = zk.getChildren(servicePathName, false);
                        	boolean found = false;
                            for(String s : list_2){
                            	if(s.contains(data[1]))  {                            		
                            		byte[] messageB = zk.getData(servicePathName +"/"+s, false, null);
                            		new Thread().sleep(3000);
                            		System.out.println("Concluido!");
                            		new Thread().sleep(1000);
                            		
                            		zk.setData(data[2],messageB, -1);
                            		found = true;
                            		break;
                            		
                            	}
                            }
                            if(!found) {
                            	System.out.println("Concluido!");
                            	String message = "Not Found!";
                            	byte[] messageB = message.getBytes(StandardCharsets.UTF_8);
                            	new Thread().sleep(5000);
                        		zk.setData(data[2],messageB, -1);
                            }
                                                		
                        	
                        }
                    	zk.delete(queuePathName +"/"+ minString, 0);                    	
                    	new Thread().sleep(5000);
                    }
                }
            }
    	}    	
    	      	
    	
    }
    static public class Election extends SyncPrimitive {
    	
    	/*construtor - Cria um Znode "/Election" na raiz caso não exista*/
    	
    	Election(String address, String root){
    		super(address);
            this.root = root;
            
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
    	}
    	
    	/*registra sua candidatura*/
    	String regCand() throws KeeperException, InterruptedException {
    		String pathName;    		
    		pathName = zk.create(root + "/" + "cand-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    		//Barreira: eleição INICIAL será liberada somente se existir 3 candidatos
    		Stat s = zk.exists("/Leader", false);    		
    		while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, false);
                    if (list.size() <3 && s == null) {
                        System.out.println("Barreira: Aguardando um numero minimo de candidatos zzzzZZZZ");
                        zk.getChildren(root, true);
                        mutex.wait();
                        new Thread().sleep(1000);
                    } else {
                    	break;
                    }
                }
    		}
    	
    		
    		return pathName;
    	}
    	
    	/*Realiza a eleição:*/    	
    	Integer makeElection(String pathName) throws KeeperException, InterruptedException {
    		List<String> list = zk.getChildren(root, false);              	 
            System.out.println("Realizando eleição - Candidatos: ");
            System.out.println("List: "+list.toString());
            Integer min = Integer.valueOf(list.get(0).substring(5));
            String minString = list.get(0);
            for(String s : list){
                Integer tempValue = Integer.valueOf(s.substring(5));
                if(tempValue < min) { 
                	min = tempValue;
                	minString = s;
                }
            }
            
            if(Integer.valueOf(pathName.substring(15)) != min) {
            	System.out.println(minString+" se tornou o lider!");
            	synchronized (mutex) {
	            	while(true) {
	            		Stat s = zk.exists("/Leader", false);
	            		if(s != null) {
	            			break;
	            		} else {
	            			new Thread().sleep(5000);
	            		}
	            		
	            	}
	            	zk.exists("/Leader", true);
	            	System.out.println("Dormindo até a proxima eleição!! zzzzzZZZ");            	
	            	mutex.wait();
	            	System.out.println("Acordei, Hora da eleição!");
            	}
            	
            } else {
            	System.out.println("Eu sou "+minString+" e me tornei o novo lider!");
            	
            }
            
            
            return min;
    	}
    	
    	synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
                mutex.notifyAll();
            }
        }
    	
    }
    
    
    
    
    
    
    
    static public class dnsResolver extends SyncPrimitive {
    	
    	/*construtor - Cria um Znode "/Dns" na raiz caso não exista*/
    	
    	dnsResolver(String address, String root){
    		super(address);
            this.root = root;
            
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
    	}
    	
    	/*registra seu serviço e ip no ZNode DNS*/
    	boolean registerService(String servName, String ip) throws KeeperException, InterruptedException {
    		String pathName;
    		String queuePathName = "/Queue";    		
    		pathName = zk.create(root + "/" + "Request-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    		zk.exists(pathName, this);
    		Stat exist = zk.exists(queuePathName, false);
    		if (exist == null) {
    			zk.create(queuePathName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    		}
    		String[] req = {"reg",servName,ip,pathName};
    		//juntando strings de req numa só e usando separados ";"
    		String reqJoin = String.join(";",req);
    		//encapsulando reqJoin em bytes armazenar no znode:
    		byte[] data = reqJoin.getBytes(StandardCharsets.UTF_8);
    		zk.create(queuePathName+ "/" + "Request-", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);    		
            System.out.println("My path name is: "+pathName);
            System.out.println("I'm waiting for my request to be processed");
            latch.await();
            System.out.println("Is Done!! Closing...");
            new Thread().sleep(1000);
            return true;
    	}
    	
    	/*Tenta resolver um endereço: Cria uma lista com todos os filhos de "root" (DNS) e itera
    	 * em cada elemento para descobrir se o serviço procurado está registrado e se sim, retorna
    	 * seu ip*/    	
    	String resolverService(String service) throws KeeperException, InterruptedException {
    		String pathName;
    		String queuePathName = "/Queue";    		
    		pathName = zk.create(root + "/" + "Request-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    		zk.exists(pathName, this);
    		Stat exist = zk.exists(queuePathName, false);
    		if (exist == null) {
    			zk.create(queuePathName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    		}
    		String[] req = {"res",service,pathName};
    		//juntando strings de req numa só e usando separados ";"
    		String reqJoin = String.join(";",req);
    		//encapsulando reqJoin em bytes armazenar no znode:
    		byte[] data = reqJoin.getBytes(StandardCharsets.UTF_8);
    		zk.create(queuePathName+ "/" + "Request-", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    		System.out.println("My path name is: "+pathName);
            System.out.println("I'm waiting for my request to be processed");
            latch.await();
            System.out.println("Is Done!!");
            byte[] data_2 = zk.getData(pathName, false, null);
            String res = new String(data_2, StandardCharsets.UTF_8);
            return res;
    	}
    	
    	synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
            try {
            	String path = event.getPath();
            	if (event.getType() == Event.EventType.NodeDataChanged) {
            		latch.countDown();			
			    } 
            	else if(event.getType() == Event.EventType.NodeDeleted) {
            		latch.countDown();
			    }
			} catch (Exception e) {e.printStackTrace();}
            	
            }
        }
    	
    	
    }
    
    public static void main(String args[]) {
        if (args[0].equals("qTest"))
            queueTest(args);
        else if (args[0].equals("barrier"))
            barrierTest(args);
        else if (args[0].equals("lock"))
        	lockTest(args);
        else if(args[0].equals("dns"))
        	dnsTest(args);
        else if(args[0].equals("leader"))
        	leaderTest(args);
        else if(args[0].equals("election"))
        	electionTest(args);
        else
        	System.err.println("Unkonw option");
    }

    public static void queueTest(String args[]) {
        Queue q = new Queue(args[1], "/app3");

        System.out.println("Input: " + args[1]);
        int i;
        Integer max = Integer.valueOf(args[2]);

        if (args[3].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++)
                try{
                    q.produce(10 + i);
                } catch (KeeperException e){
                    e.printStackTrace();
                } catch (InterruptedException e){
			    e.printStackTrace();
                }
        } else {
            System.out.println("Consumer");

            for (i = 0; i < max; i++) {
                try{
                    int r = q.consume();
                    System.out.println("Item: " + r);
                } catch (KeeperException e){
                    i--;
                } catch (InterruptedException e){
			    e.printStackTrace();
                }
            }
        }
    }
	
	/*O método barrierTest():

Conecta-se ao ZooKeeper e cria/entra na barreira distribuída
Espera até que todos os processos tenham chamado enter()
Executa uma tarefa simulada por tempo aleatório
Chama leave() e espera todos os processos saírem da barreira*/

    public static void barrierTest(String args[]) {
        Barrier b = new Barrier(args[1], "/b1", Integer.valueOf(args[2]));
        try{
            boolean flag = b.enter();
            System.out.println("Entered barrier: " + args[2]);
            if(!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }

        // Generate random integer
        Random rand = new Random();
        int r = rand.nextInt(100);
        // Loop for rand iterations
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        try{
            b.leave();
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }
        System.out.println("Left barrier");
    }
    
    public static void lockTest(String args[]) {
    	Lock lock = new Lock(args[1],"/lock",Long.valueOf(args[2]));
        try{
        	boolean success = lock.lock();
        	if (success) {
        		lock.compute();
        	} else {
        		while(true) {
        			//Waiting for a notification
        		}
            }         
        } catch (KeeperException e){
        	e.printStackTrace();
        } catch (InterruptedException e){
        	e.printStackTrace();
        }
    }
    
    public static void dnsTest(String args[]) {
    	    		
    	dnsResolver s = new dnsResolver(args[2],"/Response");
    	 if (args[1].equals("reg")) {
             System.out.println("Register Service!");
             try{
            	 boolean regis = s.registerService(args[3],args[4]);
            	 System.out.println("Serviço "+args[3]+" registrado!");
            	 //while(true);
             } catch (KeeperException e){
             	e.printStackTrace();
             } catch (InterruptedException e){
             	e.printStackTrace();
             }
         } else {
             System.out.println("Resolver service!");
             try{
                 String res = s.resolverService(args[3]);
                 System.out.println("Resolucao de "+args[3] +":");
                 System.out.println(res);
                 new Thread().sleep(2000);
                 //while(true);
                 //new Thread().sleep(10000);
                 
             } catch (KeeperException e){
             	e.printStackTrace();
             } catch (InterruptedException e){
             	e.printStackTrace();
             }
             
         } 
    }
    
    public static void leaderTest(String args[]) {
    	Leader leader = new Leader(args[1],"/Leader");
        try{
        	leader.leaderProcess(); 
        } catch (KeeperException e){
        	e.printStackTrace();
        } catch (InterruptedException e){
        	e.printStackTrace();
        }
    }
    
    public static void electionTest(String args[]) {
    	Election defLeader = new Election(args[1],"/Election");
        try{
        	String pathName = defLeader.regCand();
        	while(true) {
        		int id = defLeader.makeElection(pathName);
        		if(Integer.valueOf(pathName.substring(15))==id) {
        			leaderTest(args);        			
        		}
//        		else {
//        			System.out.println("Dormindo até a proxima eleição!! zzzzzZZZ");
//        			mutex.wait();
//                	System.out.println("Acordei, Hora da eleição!");
//        		}
        	}
        	
        	
        } catch (KeeperException e){
        	e.printStackTrace();
        } catch (InterruptedException e){
        	e.printStackTrace();
        }
    }
    
    
}

    



