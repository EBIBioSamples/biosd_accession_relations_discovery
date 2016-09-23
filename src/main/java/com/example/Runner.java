package com.example;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.Resources;
import org.springframework.hateoas.hal.Jackson2HalModule;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.management.relation.RelationService;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Component
public class Runner implements ApplicationRunner {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Value("#{'${relations.unwanted}'.split(',')}")
    private List<String> unwantedRelations;

    @Value("${relations.url}")
    private String relationsBaseUrl;

    private RestTemplate restTemplate;

    private Options getCommandLineOptions() {
        Options options = new Options();
        Option output = Option.builder("o")
                .longOpt("output")
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
                "base_accessions.txt";
        String subAccession = cmd.hasOption("sa") ?
                cmd.getOptionValue("sa") :
                "ebiscims";


        String sql = "SELECT CONCAT('SAMEA',ACCESSION) FROM SAMPLE_ASSAY WHERE SUBMISSION_ACCESSION= ?";
        List<String> accessions = jdbcTemplate.queryForList(sql,String.class, subAccession);

        restTemplate = getJsonHalRestTemplate();


        Queue<String> current = new LinkedList<>();
        final Set<String> nextCheck = new HashSet<>();
        Set<String> validAccessions = new HashSet<>();

        current.addAll(accessions);

        long startTime = System.nanoTime();
        while(current.size() > 0 ) {

            current.parallelStream().forEach(accession -> {
                if (validAccessions.contains(accession)) {
                    return;
                }

                if (validAccessions.size() % 100 == 0) {
                    log.info(String.format("Collected %d accessions by now", validAccessions.size()));
                }

                String url = String.format("%s/%s/", relationsBaseUrl, accession);
                Resource<Relations> doc = getDocument(url);
                if (doc != null) {
                    validAccessions.add(accession);
                    List<Link> links = doc.getLinks().stream()
                            .filter(el -> !(unwantedRelations.contains(el.getRel())))
                            .collect(Collectors.toList());
                    for (Link link : links) {
                        Set<String> relatedAccessions = getRelatedAccession(link.getHref());
                        nextCheck.addAll(relatedAccessions);

                    }
                }
            });

            current.clear();
            current.addAll(nextCheck);
            nextCheck.clear();


        }

//        System.out.println(checkedDocuments);
        long endTime = System.nanoTime();
        log.info(String.format("Time passed %.2f seconds", (endTime - startTime)/1000000000.0 ));
        try {
            saveAccessionsToFile(output, validAccessions);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private Resource<Relations> getDocument(String url) {
        try {
            ResponseEntity<Resource<Relations>> response = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<Resource<Relations>>() {
                    });
            if (response.getStatusCode() == HttpStatus.OK) {
                Resource<Relations> body = response.getBody();
                return body;
            }

        } catch(HttpClientErrorException e){
            log.warn("Document retrieving error", e);
        }
        return null;
    }


    private Set<String> getRelatedAccession(String url) {
        try {
            ResponseEntity<Resources<Resource<Relations>>> response = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<Resources<Resource<Relations>>>(){
                    });
            if (response.getStatusCode() == HttpStatus.OK) {
                Resources<Resource<Relations>> body = response.getBody();
                return body.getContent().stream()
                        .map(el -> el.getContent().getAccession())
                        .collect(Collectors.toSet());
            }
        } catch (HttpClientErrorException e) {
            if (! e.getStatusCode().equals(HttpStatus.NOT_FOUND))
                log.warn("Relations not found", e);
        }
        return Collections.emptySet();


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

        return restTemplate;
    }

    private Map filterUnwantedRelations(Map relations, List<String> unwantedRelations) {
        Map<String, String> filteredRelations = new HashMap<>();
        filteredRelations.put("accession", (String) relations.get("accession"));
        Map<String, Map<String, String>> links = (Map<String, Map<String, String>>) relations.get("_links");
        links.entrySet().forEach(el ->  {
            if (! unwantedRelations.contains(el.getKey())) {
                Map<String, String> linkValue = el.getValue();
                filteredRelations.put(el.getKey(), linkValue.get("href"));
            }
        });
        return filteredRelations;
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
