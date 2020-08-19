package com.freelance.app.springbatchconfig;

import java.util.Date;
import java.util.List;

import javax.sql.DataSource;


import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.freelance.app.model.Order;

@Configuration
@EnableBatchProcessing
@EnableScheduling
public class SpringBatchConfigLinkidinExemple  extends QuartzJobBean {
	
	public static String[] tokens = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};

	public static String ORDER_SQL = "select order_id, first_name, last_name, "
			+ "email, cost, item_id, item_name, ship_date "
			+ "from SHIPPED_ORDER order by order_id";
	
	public static String INSERT_ORDER_SQL = "insert into "
			+ "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date)"
			+ " values(?,?,?,?,?,?,?,?)";
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	
	@Autowired
	public JobLauncher jobLauncher;
	
	@Autowired
	public JobExplorer jobExplorer;
	
	/** job run every 30 second"
	 * We will need to execute spring batch jobs periodically on fixed schedule using some cron expression passed to Spring TaskSchedule
	 * @throws JobParametersInvalidException 
	 * @throws JobInstanceAlreadyCompleteException 
	 * @throws JobRestartException 
	 * @throws JobExecutionAlreadyRunningException **/
	@Scheduled(cron = "0/30 * * * * *")
	public void runJob() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		JobParametersBuilder paramBuilder = new JobParametersBuilder();
		paramBuilder.addDate("runTime", new Date());
		this.jobLauncher.run(job(), paramBuilder.toJobParameters());
	}
	

	/**
   	  * configure Quartz scheduler to run Spring batch jobs configured using Spring boot Java configuration. 
   	  * Although, Spring’s default scheduler is also good, but quartz does the scheduling and invocation of tasks much better and in more configurable way. 
    	  * This leaves Spring batch to focus on creating batch jobs only, and let quartz execute them
	  * Trigger instances are also created to configure batch job execution time and frequency.
    	 */
	@Bean
	public Trigger trigger() {
		SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
				.simpleSchedule()
				.withIntervalInSeconds(30)
				.repeatForever();
		
		return TriggerBuilder.newTrigger()
				.forJob(jobDetail())
				.withSchedule(scheduleBuilder)
				.build();
	}
	
	  /**
    	 * Scheduling with spring batch
    	 * JobDetail instances contain information about QuartzJobBean and information to inject
    	 */
	@Bean
	public JobDetail jobDetail() {
		return JobBuilder.newJob(SpringBatchConfigLinkidinExemple.class)
				.storeDurably()
				.build();
	}
	
 	   /**
   	  * create standard QuartzJobBean instance which we will use to execute batch jobs
   	  */
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		JobParameters parameters = new JobParametersBuilder(jobExplorer)
				.getNextJobParameters(job())
				.toJobParameters();
		try {
			
			this.jobLauncher.run(job(), parameters);
			
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Bean
	public JobExecutionDecider decider() {
		return new DeliveryDecider(); 
	}

	/**
	 * Step excute one task
	 * 
	 * @return
	 */
	@Bean
	public Step packageItemStep() {
		return this.stepBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				/**
				 * get Params job
				 */
				/**
				 * String item =
				 * chunkContext.getStepContext().getJobParameters().get("item").toString();
				 * String date =
				 * chunkContext.getStepContext().getJobParameters().get("run.date").toString();
				 * System.out.println(String.format("this %s has been packaged on %s", item,
				 * date));
				 */
				System.out.println("the item has been packaged");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step driveToAdressStep() {
		return this.stepBuilderFactory.get("driveToAdressItemStep").tasklet(new Tasklet() {
			boolean GOT_LOST = false;

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				if(GOT_LOST) {
					throw new RuntimeException("Got lost drive to this adress");
				}
				System.out.println("Succefully arrived to adress");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step givePackageToCustomerStep() {
		return this.stepBuilderFactory.get("givePackageToAdressItemStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Giben the package to customer");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step storePackageStep() {
		return this.stepBuilderFactory.get("storePackageItemStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Storing the package");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step leaveAtDoorStep() {
		return this.stepBuilderFactory.get("leaveAtDoorStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("leave at door");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
    
	@Bean
	public ItemReader<Order> itemReader() {
		/** Read from CSV file  
		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<Order>();
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new FileSystemResource("src/main/resources/shipped_orders.csv"));
		
		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<Order>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(tokens);
		
		lineMapper.setLineTokenizer(tokenizer);
		
		lineMapper.setFieldSetMapper(new OrderfieldSetMapper());
		
		itemReader.setLineMapper(lineMapper);
		return itemReader;
		**/
		return new JdbcCursorItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.sql(ORDER_SQL)
				.rowMapper(new OrderRowMapper())
				.build();	
	}
	
	@Bean
	public ItemWriter<Order> itemWriter() {
		return new JdbcBatchItemWriterBuilder<Order>()
				.dataSource(dataSource)
				.sql(INSERT_ORDER_SQL)
				.itemPreparedStatementSetter(new OrderItemPreparedStatementSetter())
				.build();
	}
	
	@Bean
	public ItemProcessor<Order, Order> orderValidatingItemProcessor() {
		BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<Order>();
		itemProcessor.setFilter(true);
		return itemProcessor;
	}

	
	@Bean
	public Step chunkBasedStep() {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order, Order>chunk(3)
				.reader(itemReader())
				.processor(orderValidatingItemProcessor())
				.writer(itemWriter())
				.build();	
			
	}
	
	@Bean
	public Job job() {
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				.build();
	}


/**	@Bean
	public Job deliverPackageJob() {
		return this.jobBuilderFactory.get("deliverPackageJob")
				.start(packageItemStep())
				.next(driveToAdressStep())
				/** Batch status failed if step not succed job wil be failed or stped by stop()**/
	/**				.on("FAILED").fail()
				.from(driveToAdressStep())
				/** decier is a class have conditions**/
		/**			.on("*").to(decider())
						.on("PRESENT").to(givePackageToCustomerStep())
					.from(decider())
					     .on("NOT_PRESENT").to(leaveAtDoorStep())
				.end()
				.build();
	}**/

	
	
   

}
