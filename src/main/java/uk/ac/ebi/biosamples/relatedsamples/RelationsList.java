package uk.ac.ebi.biosamples.relatedsamples;

import org.springframework.beans.factory.annotation.Value;

import java.util.List;

public class RelationsList {

    @Value("samplesrelations")
    List<Relations> samplesRelations;

    @Value("groupsrelations")
    List<Relations> groupsRelations;


}
