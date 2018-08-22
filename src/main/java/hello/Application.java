package hello;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableTransactionManagement
@EnableAsync
public class Application implements CommandLineRunner {

  private static final Logger log = LoggerFactory.getLogger(Application.class);

  @Bean(name="pool-01")
  public ThreadPoolTaskExecutor pool01() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    
    executor.setCorePoolSize(4);
    executor.setMaxPoolSize(4);
    executor.initialize();

    return executor;
  }
  
  public static void main(String args[]) {
    SpringApplication.run(Application.class, args);
  }
  
  @Autowired
  CustomerService custService;
  
  @Autowired
  ConcurrentCustomerService conCustService;
  
  @Autowired
  AsyncCustomerService asyncCustService;
  
  @Autowired
  ThreadPoolTaskExecutor taskExecutor;
  
  @Override
  public void run(String... args) throws Exception {
    
    startProcess();
    startConcurrentProcess();
    startAsyncProcess();
    
    if (taskExecutor.getActiveCount() == 0) {
      taskExecutor.shutdown();
    }
  }
  
  public void startProcess() {
    log.info("startProcess() start");
    
    try {
      custService.createTable();
      
      final String[] originalNames = new String[] { "John Woo", "Jeff Dean", "Josh Bloch", "Josh Long" };
      log.info(String.format("originalNames=%s", Arrays.toString(originalNames)));
      
      List<Object[]> splitNames = Arrays.asList(originalNames).stream()
        .map(name -> name.split(" "))
        .collect(Collectors.toList());
      
      splitNames.forEach(name -> log.info(String.format("Source data: %s, %s", name[0], name[1])));
      
      custService.insertData(splitNames);

      final String searchFirstName = "Josh";
      log.info(String.format("Customers with firstName='%s': ", searchFirstName));
      custService.findByFirstName(searchFirstName);
      
    } catch (Exception e) {
      log.error("startProcess(): " + e.getMessage(), e);
    }
    
    log.info("startProcess() end");
  }
  
  
  public void startConcurrentProcess() {
    log.info("startConcurrentProcess() start");
    
    try {
      conCustService.createTable();

      final String[] originalNames = new String[] { "John Woo", "Jeff Dean", "Josh Bloch", "Josh Long" };
      log.info(String.format("originalNames=%s", Arrays.toString(originalNames)));

      List<Object[]> splitNames = Arrays.asList(originalNames).stream()
          .map(name -> name.split(" "))
          .collect(Collectors.toList());

      splitNames.forEach(name -> log.info(String.format("Source data: %s, %s", name[0], name[1])));

      ConcurrentCustomerService.insertDataConcurrently(splitNames, conCustService, taskExecutor);
      
      final String searchFirstName = "Josh";
      log.info(String.format("Customers with firstName='%s': ", searchFirstName));
      conCustService.findByFirstName(searchFirstName);

    } catch (Exception e) {
      log.error("startConcurrentProcess(): " + e.getMessage(), e);
    }
    
    log.info("startConcurrentProcess() end");
  }
  
  
  public void startAsyncProcess() {
    log.info("startAsyncProcess() start");
    
    try {
      asyncCustService.createTable();

      final String[] originalNames = new String[] { "John Woo", "Jeff Dean", "Josh Bloch", "Josh Long" };
      log.info(String.format("originalNames=%s", Arrays.toString(originalNames)));

      List<Object[]> splitNames = Arrays.asList(originalNames).stream()
          .map(name -> name.split(" "))
          .collect(Collectors.toList());

      splitNames.forEach(name -> log.info(String.format("Source data: %s, %s", name[0], name[1])));

      AsyncCustomerService.insertDataAsync(splitNames, asyncCustService);
      
      final String searchFirstName = "Josh";
      log.info(String.format("Customers with firstName='%s': ", searchFirstName));
      asyncCustService.findByFirstName(searchFirstName);

    } catch (Exception e) {
      log.error("startAsyncProcess(): " + e.getMessage(), e);
    }
    
    log.info("startAsyncProcess() end");
  }
}
