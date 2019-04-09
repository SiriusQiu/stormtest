import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spouts.WordReader;

import bolts.WordCounter;
import bolts.WordNormalizer;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader",new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer())
			.shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter())
			.fieldsGrouping("word-normalizer", new Fields("word"));
		
        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", "C:\\Users\\Sirius\\git公有项目\\examples-ch02-getting_started\\src\\main\\resources\\words.txt");
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		System.out.println("配置成功");
		LocalCluster cluster = new LocalCluster();
		System.out.println("开始运行");
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		System.out.println("运行结束");
		Thread.sleep(2000);
		System.out.println("准备关闭族群");
		cluster.shutdown();
		System.out.println("族群已关闭");
	}
}
