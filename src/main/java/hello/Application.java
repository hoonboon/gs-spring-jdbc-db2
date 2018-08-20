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
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableTransactionManagement
public class Application implements CommandLineRunner {

  
  private static final Logger log = LoggerFactory.getLogger(Application.class);

  public static void main(String args[]) {
    SpringApplication.run(Application.class, args);
  }
  
  @Autowired
  CustomerService custService;
  
  @Override
  public void run(String... args) throws Exception {
    
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
      log.error("run(): " + e.getMessage());
    }
    
  }
  
}
