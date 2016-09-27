package uk.ac.ebi.biosamples.relatedsamples;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.hateoas.hal.Jackson2HalModule;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;



@Component
public class Runner implements ApplicationRunner {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    private ApplicationContext context;

    @Value("${threadpool.count:16}")
    private int threadpoolCount;

    private Options getCommandLineOptions() {
        Options options = new Options();
        Option output = Option.builder("o")
                .longOpt("output-file")
                .desc("output file where to save accessions")
                .build();
        Option subacc = Option.builder("sa")
                .longOpt("sub_accession")
                .desc("the submission accession name")
                .build();

        options.addOption(output).addOption(subacc);
        return options;
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(getCommandLineOptions(), applicationArguments.getSourceArgs());

        String output = cmd.hasOption("o") ?
                cmd.getOptionValue("o") :
                "accessions.txt";
        String subAccession = cmd.hasOption("sa") ?
                cmd.getOptionValue("sa") :
                "ebiscims";


        log.info("Starting process");

        log.info(String.format("Querying database for accession with owner %s", subAccession));
        String sql = "SELECT CONCAT('SAMEA',ACCESSION) FROM SAMPLE_ASSAY WHERE SUBMISSION_ACCESSION= ?";
        List<String> accessions = jdbcTemplate.queryForList(sql, String.class, subAccession);
        log.info(String.format("Query done, retrieved %d accessions from database", accessions.size()));

        RestTemplate restTemplate = getJsonHalRestTemplate();

        ExecutorService executor = Executors.newFixedThreadPool(threadpoolCount);

        Set<String> toBeChecked = new HashSet<>();
        Set<String> futureCreated = new HashSet<>();
        Set<String> validAccessions = new HashSet<>();
        Queue<Future<CheckAccessionResult>> futures = new LinkedList<>();

//        Set<String> accessionsSubset = accessions.stream().limit(100).collect(Collectors.toSet());
//        toBeChecked.addAll(accessionsSubset);
        toBeChecked.add("SAMEA4303542");
        log.info("Started discovery process");

        while(toBeChecked.size() > 0  || futures.size() > 0) {

            for(String acc: toBeChecked) {
                if (acc != null && !futureCreated.contains(acc)) {
                    CheckAccessionCallable task = context.getBean(CheckAccessionCallable.class, acc, restTemplate);
                    Future<CheckAccessionResult> futureResult = executor.submit(task);
                    futures.add(futureResult);
                    futureCreated.add(acc);
                }
            }

            toBeChecked = new HashSet<>();

            Queue<Future<CheckAccessionResult>> notDoneFutures = new LinkedList<>();
            for(Future<CheckAccessionResult> fut : futures) {
                if (fut.isDone()) {
                    try {
                        String acc = fut.get().accession;
                        Set<String> relatedAcc = fut.get().relatedAccessions;
                        if (acc != null && !validAccessions.contains(acc)) {
                            validAccessions.add(acc);
                            Set<String> notSeenAccession = relatedAcc
                                    .stream()
                                    .filter(a -> a != null)
                                    .filter(a -> !validAccessions.contains(a))
                                    .filter(a -> !futureCreated.contains(a))
                                    .collect(Collectors.toSet());
                            toBeChecked.addAll(notSeenAccession);
                        }
                    } catch ( ExecutionException e) {
                        log.error("Execution error while retrieving future content",e);
                    }
                } else {
                    notDoneFutures.add(fut);
                }
            }

            futures = notDoneFutures;
            log.debug(String.format("Futures remaining - %d | Valid Accessions - %d | Futures created - %d",
                    futures.size(),
                    validAccessions.size(),
                    futureCreated.size()));

        }

        executor.shutdown();
        executor.awaitTermination(5,TimeUnit.MINUTES);
        log.info(String.format("Process finished, saving file %s",output));
        try {
            saveAccessionsToFile(output, validAccessions);
        } catch (IOException e) {
            log.error("Error while saving accessions to file", e);
        }

    }



    private RestTemplate getJsonHalRestTemplate() {

        RestTemplate restTemplate = new RestTemplate();

        //need to create a new message converter to handle hal+json
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jackson2HalModule());
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(MediaType.parseMediaTypes("application/hal+json"));
        converter.setObjectMapper(mapper);

        //add the new converters to the restTemplate
        //but make sure it is BEFORE the exististing converters
        List<HttpMessageConverter<?>> converters = restTemplate.getMessageConverters();
        converters.add(0,converter);
        restTemplate.setMessageConverters(converters);

        PoolingHttpClientConnectionManager conman = new PoolingHttpClientConnectionManager();
        conman.setMaxTotal(128);
        conman.setDefaultMaxPerRoute(64);
        conman.setValidateAfterInactivity(0);

        ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {

            @Override
            public long getKeepAliveDuration(org.apache.http.HttpResponse httpResponse, org.apache.http.protocol.HttpContext httpContext) {
                //see if the user provides a live time
                HeaderElementIterator it = new BasicHeaderElementIterator(httpResponse.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param.equalsIgnoreCase("timeout")) {
                        return Long.parseLong(value) * 1000;
                    }
                }
                //default to one second live time
                return 1 * 1000;
            }
        };

        CloseableHttpClient httpClient = HttpClients.custom()
                .setKeepAliveStrategy(keepAliveStrategy)
                .setConnectionManager(conman).build();

        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory(httpClient));

        return restTemplate;
    }

    private void saveAccessionsToFile(String output, Collection<String> accessions) throws IOException {

        File outputFile = new File(output);
        if (!outputFile.exists()) {
            outputFile.createNewFile();
        }

        FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        for (String acc: accessions) {
            bw.write(acc);
            bw.newLine();
        }
        bw.close();

    }


}
