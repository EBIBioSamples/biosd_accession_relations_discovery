package uk.ac.ebi.biosamples.relatedsamples;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.Resources;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CheckAccessionCallable implements Callable<CheckAccessionResult>{

    private Logger log = Logger.getLogger(CheckAccessionCallable.class);

    @Value("#{'${relations.unwanted}'.split(',')}")
    private List<String> unwantedRelations;

    @Value("${relations.url}")
    private String relationsBaseUrl;

    private String accession;

    private RestTemplate restTemplate;



    public CheckAccessionCallable(String accession, RestTemplate restTemplate) {
        this.accession = accession;
        this.restTemplate = restTemplate;
    }

    @Override
    public CheckAccessionResult call() throws Exception {
        CheckAccessionResult result = new CheckAccessionResult();

        Set<String> relatedAccessions = new HashSet<>();

        boolean isGroup = accession.startsWith("SAMEG");
        String accType = isGroup ? "groups" : "samples";
        String url = String.format("%s/%srelations/%s",
                relationsBaseUrl,
                accType,
                accession);

        Resource<Relations> doc = getDocument(url);
        if (doc != null &&
            doc.getContent().getAccession().equals(accession)) {

            List<Link> links = doc.getLinks().stream()
                    .filter(el -> !(unwantedRelations.contains(el.getRel())))
                    .collect(Collectors.toList());
            for (Link link : links) {
                relatedAccessions.addAll(getRelatedAccession(link.getHref()));
            }

            result.accession = this.accession;
            result.relatedAccessions = relatedAccessions;
        }

        return result;
    }

    private Resource<Relations> getDocument(String url) {
        long startTime = System.nanoTime();
        Resource<Relations> resourceToReturn = null;
        try {
            ResponseEntity<Resource<Relations>> response = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<Resource<Relations>>() {
                    });
            if (response.getStatusCode() == HttpStatus.OK) {
                resourceToReturn = response.getBody();
            }

        } catch(HttpClientErrorException e){
            log.debug("Error while retrieving document",e);
        }
        long endTime = System.nanoTime();
        log.debug(String.format("getDocument - %d ms", (endTime - startTime) / 1000000 ));
        return resourceToReturn;
    }


    private Set<String> getRelatedAccession(String url) {
        long startTime = System.nanoTime();
        Set<String> relatedAccession = Collections.emptySet();
        try {
            ResponseEntity<Resources<Resource<Relations>>> response = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<Resources<Resource<Relations>>>(){
                    });
            if (response.getStatusCode() == HttpStatus.OK) {
                Resources<Resource<Relations>> body = response.getBody();
                relatedAccession = body.getContent().stream()
                        .map(el -> el.getContent().getAccession())
                        .collect(Collectors.toSet());
            }
        } catch (HttpClientErrorException e) {
            if (! e.getStatusCode().equals(HttpStatus.NOT_FOUND))
                log.debug("Error getting relation", e);
        }
        long endTime = System.nanoTime();
        log.debug(String.format("getRelatedAccession - %d ms", (endTime - startTime) / 100000 ));
        return relatedAccession;


    }
}
